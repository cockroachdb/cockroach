/* Copyright 2017 The Cockroach Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * This file is derived from the hash/crc32 implementation in the Go
 * Programming Language and is thus additionally subject to the terms
 * of the Go license in LICENSE.golang.
 *
 * Author: Nikhil Benesch (nikhil.benesch@gmail.com)
 */

/* 
 * This implementation is nearly a direct translation of Go's optimized AMD64
 * CRC32 implementation [0], which uses a non-standard syntax, into GNU assembly
 * syntax. The technique uses carry-less multiplication instructions that
 * operate on two double quadword (64-bit) operands at a time, and is described
 * in detail in an Intel whitepaper, "Fast CRC Computation for Generic
 * Polynomials Using PCLMULQDQ Instruction" [1].
 *
 * [0]: https://github.com/golang/go/blob/f3e6216450866f761cc22c042798c88062319867/src/hash/crc32/crc32_amd64.s
 * [1]: https://www.intel.com/content/dam/www/public/us/en/documents/white-papers/fast-crc-computation-generic-polynomials-pclmulqdq-paper.pdf
 */

/*
 * These constants are lifted from the Linux kernel, since they avoid the costly
 * PSHUFB 16 byte reversal proposed in the original Intel paper.
 *
 * NB: Each literal below is 33 bits, so each label points to 128 bits.
 */

.align 16

.Lr2r1:
  .quad 0x154442bd4
  .quad 0x1c6e41596

.Lr4r3:
  .quad 0x1751997d0
  .quad 0x0ccaa009e

.Lr5:
  .quad 0x163cd6124
  .quad 0x000000000

.Lrupoly:
  .quad 0x1db710641
  .quad 0x1f7011641

.text

/* crc32ieee_clmul_impl(uint32_t crc, const char* buf, size_t len); */
.global _crc32ieee_clmul_impl
_crc32ieee_clmul_impl:
  #define crc %edi
  #define buf %rsi
  #define len %rdx

  pushq %rbp
  movq %rsp, %rbp

  movd crc, %xmm0
  
  movdqu 0x00(buf), %xmm1
  movdqu 0x10(buf), %xmm2
  movdqu 0x20(buf), %xmm3
  movdqu 0x30(buf), %xmm4

  pxor %xmm0, %xmm1
  add $0x40, buf
  sub $0x40, len
  cmp $0x40, len
  jb remain64

  movdqa .Lr2r1(%rip), %xmm0

loopback64:
  movdqa %xmm1, %xmm5
  movdqa %xmm2, %xmm6
  movdqa %xmm3, %xmm7
  movdqa %xmm4, %xmm8

  pclmulqdq $0x00, %xmm0, %xmm1
  pclmulqdq $0x00, %xmm0, %xmm2
  pclmulqdq $0x00, %xmm0, %xmm3
  pclmulqdq $0x00, %xmm0, %xmm4

  /* Load next 64 bytes early. */
  movdqu 0x00(buf), %xmm11
  movdqu 0x10(buf), %xmm12
  movdqu 0x20(buf), %xmm13
  movdqu 0x30(buf), %xmm14

  pclmulqdq $0x11, %xmm0, %xmm5
  pclmulqdq $0x11, %xmm0, %xmm6
  pclmulqdq $0x11, %xmm0, %xmm7
  pclmulqdq $0x11, %xmm0, %xmm8

  pxor %xmm5, %xmm1
  pxor %xmm6, %xmm2
  pxor %xmm7, %xmm3
  pxor %xmm8, %xmm4

  pxor %xmm11, %xmm1
  pxor %xmm12, %xmm2
  pxor %xmm13, %xmm3
  pxor %xmm14, %xmm4

  add $0x40, buf
  sub $0x40, len
  cmp $0x40, len
  jge loopback64

  /* Fold result into a single register, %xmm1. */
remain64:
  movdqa .Lr4r3(%rip), %xmm0

  movdqa %xmm1, %xmm5
  pclmulqdq $0x00, %xmm0, %xmm1
  pclmulqdq $0x11, %xmm0, %xmm5
  pxor %xmm5, %xmm1
  pxor %xmm2, %xmm1

  movdqa %xmm1, %xmm5
  pclmulqdq $0x00, %xmm0, %xmm1
  pclmulqdq $0x11, %xmm0, %xmm5
  pxor %xmm5, %xmm1
  pxor %xmm3, %xmm1

  movdqa %xmm1, %xmm5
  pclmulqdq $0x00, %xmm0, %xmm1
  pclmulqdq $0x11, %xmm0, %xmm5
  pxor %xmm5, %xmm1
  pxor %xmm4, %xmm1

  cmp $0x10, len
  jb finish

  /* Encode 16 bytes. */
remain16:
  movdqu 0x00(buf), %xmm10

  movdqa %xmm1, %xmm5
  pclmulqdq $0x00, %xmm0, %xmm1
  pclmulqdq $0x11, %xmm0, %xmm5
  pxor %xmm5, %xmm1
  pxor %xmm10, %xmm1

  add $0x10, buf
  sub $0x10, len
  cmp $0x10, len
  jge remain16

  /* Fold final result into 32 bits and return. */
finish:
  pcmpeqb %xmm3, %xmm3
  pclmulqdq $0x01, %xmm1, %xmm0
  psrldq $0x08, %xmm1
  pxor %xmm0, %xmm1

  movdqa %xmm1, %xmm2
  movdqa .Lr5(%rip), %xmm0

  /* Create a mask to select the lower 32-bits; we don't care about the
   * upper 96 bits. */
  psrlq $0x20, %xmm3

  psrldq $0x04, %xmm2
  pand %xmm3, %xmm1
  pclmulqdq $0x00, %xmm0, %xmm1
  pxor %xmm2, %xmm1

  movdqa .Lrupoly(%rip), %xmm0

  movdqa %xmm1, %xmm2
  pand %xmm3, %xmm1
  pclmulqdq $0x10, %xmm0, %xmm1
  pand %xmm3, %xmm1
  pclmulqdq $0x00, %xmm0, %xmm1
  pxor %xmm2, %xmm1

  pextrd $0x01, %xmm1, %eax

  leave
  ret
