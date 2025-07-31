--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.9
-- Dumped by pg_dump version 9.6.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;


--
-- Name: simple; Type: EXTENSION; Schema: -; Owner:
--

CREATE EXTENSION IF NOT EXISTS simple WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION simple IS 'simple extension';

--
-- Name: simple; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.simple (
    i integer NOT NULL,
    s text,
    b bytea
);


ALTER TABLE public.simple OWNER TO postgres;

--
-- Data for Name: simple; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.simple (i, s, b) FROM stdin;
0	str	\N
1		\N
2	 	\N
3	,	\N
4	\n	\N
5	\\n	\N
6	\r\n	\N
7	\r	\N
9	"	\N
10	\N	\N
11	\\N	\N
12	NULL	\N
13	¢	\N
14	 ¢ 	\N
15	✅	\N
16	","\\n,™¢	\N
19	✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑	\N
20	a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "\n	\N
21	"a slash \\, a double slash \\\\, a slash+quote \\",  \\\n	\N
100	<ॹ\t\\"\tᐿ\\π�✅<✅\r\t"<,aॹॹ\\ᐿ✅<"aॹ�\n�a\nॹ\t"\t"\\�;,\r�ॹᐿ\t"",,✅,\n;<,\n\nॹa\n\n\\ᐿᐿ,✅πa;;��\na✅✅;"\n<;,✅,✅✅�\\,ॹ,\t\tॹ,a<a<\r✅\n\\ॹॹaπ\ra��<✅;<\\,✅<;�ππ<ᐿa\tॹ�πᐿ✅\n";a,<\nᐿ\\\n✅\n<\t\n"\tॹ\n"✅ᐿ\n✅<a✅\rπ;\t,a\n\t\t\\\n\n\\"\t<\t;a<�;,\\,π,,,<\\�,aπ\\;\n\t\nॹπ\n"✅πᐿaaॹ✅,,,,"ᐿ;ॹ\\\t\r\t"\tᐿπ;✅✅π<✅\r\\""\\�\tॹπaa,\nᐿ\r,"\t,ᐿ\n\n"ᐿ\n,\\✅\r\\\rπ\\�\n\r\\π\n"✅π\r\n\r<\r\\\n\n",\n<ᐿᐿ✅\rπ�π\n\n\\ॹ\n"�"ॹॹॹ✅\t<<ᐿ�\\"ᐿ\nॹॹ;�;"✅\nᐿॹ;"\\\\π\naॹ\tᐿ\r;ॹ✅\nᐿ\r\r"��\n;\n\t;\t\n"\n\r\n;<�π<<a<<\ra\r�";�,\n\n\t<�\n�\n<\n\n<�\raa\r\r\n<�ॹ\n;\\✅\nπ\r;<�✅ॹ\na<,ππ,a�ॹ"\n<π\nᐿ\n✅,\n\t\\\\���ᐿ",\n\n\t<ॹ✅\tᐿa\\<ॹ\r✅,\n;"\nπ\t<,✅ॹ<�<�,,,✅✅\t\tᐿ\t\r\\ॹ,ᐿ,\n✅,�aॹ\na\n;ᐿ\t�\t�\t;;,\r\rॹ"\n\r�\r�\t\r\r;";\n\n"\t\raπ�\n\ta\nπ,\n\n\\\tπaॹ�✅,\\"<\n\ra\r\n\\;;a�;\n"\rॹ\r�ᐿ�;,,ॹ✅;\tπ\t\tπ✅✅�"✅�\nᐿ✅ॹ�ᐿaa\nॹ<\n",<<�ᐿa,ॹ<\r\n;π\tᐿa\n<<\t,;π\n\rᐿ<✅a\naa\n;�\t<ॹᐿ\tॹ<<ᐿ"<<\n,\t,a;�ᐿ\n�✅�\\ᐿ""a\n\rॹ,;<a\n\tπ✅<\n��a,""\r\r✅;\\\ta,\n✅ॹ�\\,\t,\r<ᐿ;\n;\t�""ॹᐿ\t\\��<ॹ\\ᐿ\t�\\�aa✅ᐿ\rᐿ;ᐿ\ra<"\t\r\nॹ"<;;\rॹ\\πॹᐿ�<\n;ॹ<;<"<�<ᐿ"\\�ॹ,,✅\\\\\t,\r✅;a;\nπ\n,\t\r\r;\r;"\n<\t<a\nॹa\\\r;"\t,ᐿ✅✅\\\r\rπᐿ;,\n\tᐿ"�✅�ॹa✅<✅π;πᐿa"ॹ\\\t\r✅ॹॹ\r\n�,\n,"\\a✅,<a\\"a;✅"a;\raπ\t,π,;πa\t;<π\r,ॹ;ॹ\\✅<ᐿ\r\na;,;ॹॹ✅\r\\✅;\n�✅✅,""✅\\ᐿ\r\t\n\tπa✅\\;π,ᐿa"ॹa"\tπ<�ᐿ\n\tᐿ,\n\n\rπᐿ\n\n\t\n;π,\\\n"ᐿ;ॹ\n\\ᐿ✅\tᐿ\r\n\\a\r\\aπ\\ᐿ,ᐿa<\t,;π\t\r\tॹ\t;\rॹ",ᐿaॹ\n\t\r\t;;;<ॹ\n�";\t✅ᐿ\t"ᐿ;\\\n	\\x0194fdc2fa2ffcc041d3ff12045b73c86e4ff95ff662a5eee82abdf44a2d0b75fb180daf48a79ee0b10d394651850fd4a178892ee285ece1511455780875d64ee2d3d0d0de6bf8f9b44ce85ff044c6b1f83b8e883bbf857aab99c5b252c7429c32f3a8aeb79ef856f659c18f0dcecc77c75e7a81bfde275f67cfe242cf3cc354f3ede2d6becc4ea3ae5e88526a9f4a578bcb9ef2d4a65314768d6d299761ea9e4f5aa6aec3fc78c6aae081ac8120c720efcd6cea84b6925e607be063716f96ddcdd01d75045c3f00
101	a;,\n\n✅\nॹ;a\rᐿπ",ᐿॹᐿ"",ॹ;✅✅ππ\t,ᐿ\\\naa\r;\t\n\n\t\nᐿ\r\n\tॹ✅<ᐿ;\\,,<ᐿ\n"\t\t\n<\t;\r<aᐿॹ,π\ta;a\n\n\\ππa<,ᐿ;\n✅<\n<\r✅\n<\r✅;a\t;"\rॹaπ\\ॹ"\t\t✅,✅\n<<\n�\\\n\n\nπ;""\\ᐿ\ra<\t<\rᐿ✅ॹ,ॹ,,ᐿ\ra\\a�<<a\r�π,π✅\\,a✅<π\\\n�a,;\\\r\\\n✅ॹ\n<\n\\�ॹ<ॹᐿ\r\t,ᐿ\r"\n<ॹa;\tπ\n,π;;\nπ\\<✅\nππ�✅✅a\r\r\n\r\\�<ᐿ\r"π�π��a\nπa�ᐿ\\,\na�"\\;\t<"a✅ॹ✅,\r\tॹπॹ;\n;\\<"\n"\r\na\rᐿ\t\r✅\n\t,ᐿ\t""π�;<\nॹ✅,✅\r,✅\n<"✅;\nᐿᐿ\r;<ॹ\r;a<"ᐿ<a","\r<\ta\nᐿ\\a\ra\n✅\r\raπ"\n\n,\\,\\,\r\\,"ॹ,ॹ,\nᐿ,\n;\t,\n✅\na\nॹॹ<"ᐿπ;\n\n";,,;aॹa✅\r✅�a"�<"ॹ\t�\\a��a✅\n;;π✅ᐿ\\<<aॹॹ\tॹ"\n\\\\\r✅✅,✅ॹ\t�π\\\\�,π�<a;\tᐿ✅"✅\n\n\na\r<;✅ᐿ\\✅\rπ,;;aπॹᐿ"�\\\r\r"ᐿ\n\n,\tπ;"a"π;\\\r�✅\n\ta;\n✅�\n"\ra\\ᐿ"π\r�\n\r\t;\n;π\nᐿॹᐿ\n✅πᐿπॹ"<,\n<✅\n\r✅"\n"�ᐿॹ"�a�\t\\�a\n\t\r\t<✅<ᐿ\naaπ<ॹ\n\t;ॹॹπ\rॹᐿa"\\✅\t\\ॹ\n"πॹ\t;\n;\\\t\n,;;,✅ᐿa\n;\r\\ᐿa\n\t\t<\\π\nᐿ\r<✅",\n\rπ\n�✅,,πᐿ\\π<a\r✅a�π;\n\r<\\�\t\nᐿ<,aᐿᐿ;\r\t\nॹ<<<✅π\n\naa,π✅<ॹπ"ॹ\t\n,;\\aπ<",�\\πᐿππ�,\r<ᐿ;;"\\\\\nᐿ�\t\r\\\\;\n,ᐿ\n<�ᐿᐿ\nॹ\t,aa<\n\n\nᐿ<✅"\r\n�ᐿ\n\\\n\t\\ॹ"\t\t<,"<\n;\t<\tπ;;;✅\n;,�✅ॹᐿ\t"πॹπ�<ᐿ\na\r,"ॹ\ra\n�\\",✅aa"<ππ,\t\\,;a<<<\t",✅\t";\rॹ<ᐿ�,\r,\r,"aa;a\t,\r;a\n,�\r<\t\nπ;✅ᐿ"\r\t\r\n;\r"\n<✅<\t\n\n",�\rᐿ�a\n\n\na\n�ᐿ\n,;,<"\r;a\\aa"\r\t�"\nᐿ\nπᐿπ<ॹॹ✅,\rπॹॹᐿ"��",ॹ,aπ\t\na\t�aa\n;\n✅\nॹ,\n\t\t\t\t\n\t�\rᐿ\t;✅",a;✅<ॹ�✅✅a,,\\<<\n<\r<\t<\\,\\\t,π\n\nπ\r\n✅;�✅\t\nπ<;✅\n\t";\tॹ<,\n\\\\\n\\"\t\n\rॹ✅\n<",<✅a✅✅"<\r\\ॹ\na\tᐿ"\\"ॹ	\\x52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64981855ad8681d0d86d1e91e00167939cb6694d2c422acd208a0072939487f6999eb9d18a44784045d87f3c67cf22746e995af5a25367951baa2ff6cd471c483f15fb90badb37c5821b6d95526a41a9504680b4e7c8b763a1b1d49d4955c8486216325253fec738dd7a9e28bf921119c160f0702448615bbda08313f6a8eb668d20bf5059875921e668a5bdf2c7fc4844592d2572bcd0668d2d6c52f5054e2d0836bf84c7174cb7476
102	"aπ"ᐿ;"�ᐿ\r;<\t\\"�,\t;\n\n"<ॹ"<,\n\raa;�\rπ\n<�\n�\t";\n<\\;✅✅π\\\n\r\r\\�✅\n,✅��"""�\n,ॹॹ\ra\n""π\n\t\r<ॹ"<\tॹ\r"ॹ\n,\\,ॹॹ�<ॹ\na\\ᐿᐿ✅✅a"\\\r,<\nπ\n;\\\n\\\r\n�\n�\n�\n��\t�\t\n;ᐿa\nπ,\n\n,\t<�;✅πᐿ�aa\n,;<✅\t",;",�\nᐿ�\tπ<\n\n<\t<ᐿ\\ॹ✅<�\n"π\\,�a\rᐿπ✅✅ᐿ\n,�π�,\nπ\n\n<π,\r;✅�<a\t,\t\t\n\tॹ<π\n"\\\t"ॹ"ॹ✅\\ᐿ,;ॹ�<✅\n"ᐿ<\nॹ✅\n"\n"a,\\✅\\\r",✅π;ॹ✅<�\r"<\t,\n<✅\nπॹa\\\nॹ";,\t"✅,,,\r\t\r<✅;ᐿ✅ॹ�π\n"",,"\nॹᐿaπ,ॹ,ॹ,ᐿ\\,aॹ,\na✅π\\✅"πॹ\n\r;π<ॹ✅"\t\\\r,",\\\nॹ�\r\r\n\r\\,ᐿ✅;\na\\aॹॹ\n"�ॹ✅ॹa✅ᐿaπ\r\n"\t,"\n,,,a\r\\\\π✅\\\ta\n\t✅\n<;�πॹ",πaa�\r\\<"\nॹπ\t\\<\\\nᐿ✅,\t,ॹπ\n\\✅ᐿ,\t\r,\\,a✅�\\aπ�✅\t\\\\,";a\n\\\ra<\tᐿ\t\n<;\r\nᐿ✅πa,<<"ॹ\\\\�\tॹ\n;a,\n\\<π\n✅\n✅\n\t"\nॹa<ॹ,\\\n<ᐿaᐿ"�<aᐿ\\\\\\a�\r\n\r\tᐿ\r\n\n\r✅\t\n<✅"<ॹ\r;ᐿ;�\n,\tπ<\r,ᐿ"a;\n\rॹ"\n\tॹ,ॹ�,π"\\aa\n\n<<"<ॹ\nπ\r\n;\\<�\nॹ;,"�<ॹ"\n"aॹ�\nπ,<ᐿᐿ<πᐿ\\\t<π✅\n;\rᐿ,��π\t\n,"a\n\n;π�ॹπ"\r,ᐿ✅�;✅,"\rᐿ;\n\n,\ra"\\\n\t\n\\\n\t"ᐿ\\\nॹ\t"✅\r\nπ\n�\n\tπ,\\,✅a<<aᐿ<\nπππ\t\n,π\\;<ᐿ\t"�<\n\r,\raπa\r;,<\t\n✅;πa\nᐿ;,\n\n;<";ॹᐿ✅;\\\nᐿπ"\n;\r\nॹ\nππa<ॹπ\nᐿ\ta\\\raᐿॹ"ᐿ\nॹᐿ\n\r";\\\n<,ᐿ�a�πππ\\<<a"ॹ\\<\\ᐿ\n\r\\",π\n;;�ᐿ\\<ॹ\t,�ॹ\naa<\t\n"\t\\ᐿ<ᐿ\r\n;ᐿ,ᐿᐿ\n;\\a\r;π\n�<✅�\t\t;✅\n\rπ<✅ॹ,\r�\\�"\nᐿπ;π\t<ॹ,<ॹ\t\n\n\t<π<<a\\\nᐿ\n\n<"✅\t\t\t\\"�✅\n\n\t"a\r\n;"�,<π\n\t<\r,,\t\t\\ॹ;ॹ�ᐿ<π;ᐿ;,�✅ॹ\t✅<\r<;ॹॹ<\t\\\\��ᐿ\\✅\t\r;\tॹ\t\tॹ\n"<ᐿ\n"π"✅a\n\t✅\n,✅ᐿ"\n;\n;✅\r\\✅π<ॹॹ\\ᐿ<\n✅<π✅\n\\",<a\\\rॹ\n<"\n"�,,\n✅	\\x2f8282cbe2f9696f3144c0aa4ced56dbd967dc2897806af3bed8a63aca16e18b686ba0dc208cfece65bd70a23da0026b66108fbad0844363fe09dd6a773e21b8236a37f8283efb27367f6ee35437869c4043725d5ea2c63b01af2fcbb387de40daac6225423c14a994dda08f399b7888fcb6c84703dd101ac77cf000e49b2a33f748a9d6993340fe25a5f58f01766fd3466668e9e02d727a2b49f44691178d97e75e4fc0a9ca5103b928c58066d2aaf55a4ecaefd462a35a1fab5f8e47e865b0f7f37aa169dd0c93
103	π�<a\n\n<<\\\n"\t\r\t�aॹ✅�ᐿ\r\n"\r;a\\ॹ\tᐿ\t<,\\",ᐿ�a�;<ᐿ✅�ॹa\n�\\<\\<\\�πaπॹᐿ"\n�\raᐿa\t✅✅✅\r,\n\t\r<ॹ\r"ॹ�\n\t<\ra✅<"\r�;ᐿॹ\r\\π"✅\n<π\n�π;a\t\t\n\\\n\n;a�\r"ᐿ\\ᐿa\\�\r�"�\n<"\t✅ॹ\t,✅\n�;��<"\\\\π�;✅;<\t\n\\\r\t\r✅,���\\�ᐿ\n;a�✅\nᐿ\n\t,�\r\\"\tॹ<�\\;\n�\r\tπ\\;✅,✅\r\n\rᐿ;\\\\\n\t�,<ॹॹ\\,;\n\tᐿπ\r\t✅�ᐿ;;\\ᐿ\\ॹॹᐿπ\n<aॹ\r<\na;ππ\nᐿ\r"π\t"\naॹa\\\\<a\tᐿ"ᐿᐿ"\n";,,✅ᐿ\n\\<ॹ";✅a<\ta\t;✅\r;<\t\t,π�π\n\n,\\\n"\n\r;\n"\rπॹ\r✅ᐿππ<π\n✅�\t;�π\\ॹ;ॹ"πॹ,,\n<ᐿ,\t�\n\t\n",\t\rπ;a\n�\n;<ॹaᐿ";<\n�;;ᐿ\rॹ\n,ᐿ�ᐿ<\t�aᐿ\n\n\n;ॹ",<\r"π\n;\\"a\n\r�\\ᐿ,<ॹ\t;<\n;✅"✅a;aπ\rᐿ"\\"��π\nπ<✅ᐿ<\r\t<,✅\n\n<\n"a,�\na\\✅\n\r<\\\tᐿ\n✅✅\naॹa\r"π\ra\n✅<\\✅"\tπ\\ॹ,\r\\<�ॹ\r\n✅\n\n;\\ᐿπ✅a\n\raॹ<",\\\\"";ᐿ;<ॹ"π\\\naaπ\t,a\\\n\n,\t\n,"�\tπ\n\\,,\n;�\t\r<\tπ\tπ"ππᐿᐿ<;"ᐿ\\\n\r\r<<ॹ\n,aॹπॹ�\r\n\t✅;ॹ<a✅�\t\r;✅ॹ;ᐿ"ॹ,\r"\t;�;<<\n\n;aa\\\r"✅aπ;\n",a<\n�<�\nπ\r,\n\\\n<;ᐿπ\r\n�\n"\r\\<\\ᐿ"<ᐿ�\r\nॹ\n<,"\nॹ<a��;aᐿa;\\��\n,\t;<ॹ\ra\r,";<\nᐿ\\\r✅a\r,"\nᐿॹ✅�✅\n\n\t✅\nπ�\n;π\n✅,ᐿᐿ;\ra;<ᐿॹ\r;�\\\n\n\n�\nπ�✅\n\n;";a\t"\n\\\n"\t\r\rπ\\�<✅;\n",ᐿ�\t✅<;,ᐿπ\r�✅,\\a"πa,;ॹ";,ᐿ;\n",�ॹ\rπᐿ\\✅aᐿ\\\n,ॹ✅ᐿ\nᐿ";;\n✅ᐿ;<✅\n;✅\nॹ\naa\nᐿ�\n�<\t\nπ\nॹॹ\\\n;�<�"πᐿ\\\\π\r\\ॹ,ॹॹ;\\�,�a;\r\\;π✅,a,\t,\n�✅ॹ\\"a\raॹ�\n✅a�;"\nᐿ✅\r,aa"�π<\r""\nॹ<\n\nᐿ\n;\n\\,\r,�\\<\\aπ�✅ππ,\n\\\\<;ᐿ�;✅\r\r\n\n�\n,<\rπᐿ\r✅✅\t✅�ॹ<,\nπ<ॹ\r\n,\\<\rॹॹπॹ\n\\<",ᐿ"ॹπᐿ✅π\r;✅"\n\nπ\tॹπᐿ;�;\r\r\\ᐿ\t\\�\r\r"\nᐿᐿ�;��\n\r�"✅a;	\\x85fbe72b6064289004a531f967898df5319ee02992fdd84021fa5052434bf6ee214b5fdf1409fc2b8a0a521c221bacb1bca8a3c1495ddbfbdc0b7d75b87b9cf75860b72bbef59336471c22e5d677c563eece4dd88ae65655e5a094e9cef2fb2774b795b2e4e12e15edb17907cfe1c307a187e3a99ae6ed15628da806c3b41d82393d72c9537c8275f85650e1dada2c1489050a06d37841b74bcbbdf8987a19dcddc8e966bffacfac14dab33951a9e9a4cffa46c5f60c453b5b468f20c4bd22dfcdf6d3d1426f8543
104	;π,\\✅✅ᐿπ✅,�a\r<\nπᐿॹ;π\\,✅\nᐿॹ✅�,\r�\r\r\r,;\r;ᐿ,\n\nᐿaπ\r,,✅\na,a\\<✅"✅\\,,a"π\r\n�✅π"ॹπ;\nπ;<,<\n;<\n\tॹ\rπ\r,a\\\t�\n\r\\ᐿ<\t,\n\\ᐿa\t\t\n\nπ\t\\\n\\πa;π\r\rᐿ",a<"\n�\r\ta\r\t�\r\t✅\t;ᐿᐿ��\nᐿᐿ\\,ᐿᐿ\na"ᐿ""aa\n;✅π\nॹ\\""�✅ॹॹ\\\nॹ\t\nॹ✅,\n"πᐿ\n\n;<<;\r\tᐿ�,,\\\n\n\n\n\nπ�;\n;,"✅\r;a\n\\;aa\n\n\n;\n\n<<ॹ�\nπa�πॹaॹ\r\n;✅✅,;ᐿ\n;π"\\πᐿ\n"\n\\a\\aπ✅ᐿ\n<",\tॹ\r<;";\nππ"�\n\n\t,π\\\\<\\\t;ॹπ\\,;�✅ॹ,\r<\n✅�;ᐿ",;✅\n\nॹॹ\r\n✅\n<<\n"",a\t\r\r,ॹ\t�;�,π\\\t,;\\"✅\t\n\n\nᐿ\n<\\\rᐿ,;�\nπ\r\\<ᐿᐿ\n<✅ᐿaॹ�✅\n\t,π"\r<<\n\nπ\tπ✅\n<\\πॹaᐿ\t;�ॹॹ,"\n\\a\n,"πॹ,\r,ॹॹ\\";�\n\\π✅\n"ππ\n✅�πॹ,\r✅\n;π;ᐿ"\nᐿ✅\rॹ;;\n"ॹ"�"a\n\rπ\n<\n\t"aπ\t;\\\n";"π,\ta\t\n\nᐿ<,;�<ᐿ"\\ᐿᐿa,\n;;ॹॹ\tॹπa�ᐿ\ra,π<✅\tᐿᐿ\n,✅\ra�"\r\r",;π�<;\n<;ᐿ"�;ᐿ;�;✅\t\\<\\<;πa✅\rॹ\\\\\\ᐿ\n;\r\t\n\\\r"✅\n\tπ✅""<\r\rπ\r<,\n,\\✅ᐿᐿ�\t�,ππ;ᐿ\t�"\\ππ"ॹ,πa<\n\n<��\rॹॹ\t,\r"ॹ✅✅\n\n;\\ॹ;π<"�\t�<"ᐿॹॹ;\n<\n\r\na\t�ॹᐿa\n,"\t\r"\n,\r<,"\tᐿ\\\n<,;<"\t\n\nᐿ,ᐿ\tπ✅\n,\r,\n\t<,�\\;<\\a\nπ\t,\t�ॹ\t\n�a✅\n✅\nॹ";\r\t✅<\tᐿ\n\tᐿॹ✅"\r\rॹ✅π\n\n,\t\\\t\\;"a\t,ॹॹ"aॹ\n,\n✅�\t\nॹᐿ\n\r✅<πॹ\n✅\tॹ"ॹ"�\r\\;\\✅;ॹπ;\n\nᐿ<\r<"ॹ\n,\n;π\nॹ\ta✅\n�;ᐿ"a�✅π\r✅ॹ,\n\n",✅\nᐿ\n<�\r\nπᐿ"πॹᐿ\r�\n<,✅a\\ॹ\r✅<;πᐿ✅ॹ<"<✅"π,\\\rπ\\<"<"π\n✅<;\\�\tॹ\n\n\r<\n\rᐿ\nᐿaॹaॹ\\\r<<\n\r\n�\ta,\nॹॹᐿ\n,π✅<;\\\nπॹπᐿॹ<;"a\r<;\t\t<,;�π\n<✅ॹॹ\tᐿ\rᐿaaaॹ\t\\,ᐿ✅\n\\ॹ<"π\t\r"\tᐿ\n\ta\t,<ππ;\n\\\r�\n,\n\n\\ᐿa\nॹᐿa\n\t\n\t\n✅"ᐿ"\r\n\n"�\r\n\n<<π\ra✅\\<ᐿ�\n\n✅�a✅�	\\xe2807d9c1dce26af00ca81d4fe11c23e8eb6752e1f9ad716c61fc24f2d80c04189b3a4c3f477689d0ac9a542f9b174192a2c16da483de16a3a093f9107cdc35f97f4378037ad8aa15ea7c95db087c51c99644230bb8f8b6243b21cdcc015237564a9fb2ac359aa7ab99544cd62e240885533aed411c87c530b7107321db580938d8b78eb063b5c3c4f18926cba3bc05a65244dab6d79345fe5e99adf9ddd3d1dbfe5db7f8f20aacd5ce70992f8161755f872054a64703dbfbab3be3dc57fcb075ef625033f810cda
105	<a✅ᐿ<\n\n\nॹ",\n"πॹᐿ,\rᐿ\nॹ�ॹ<\naᐿᐿ,"ॹ"\\✅\n�✅<\n\r<\\<"\\π;<✅,;✅ॹa\r✅ᐿ;ᐿ\r\\�\\�,ᐿ\r\n,�✅,\t;\\"π,;ᐿa\nॹ✅\n;ॹ<\\\n;<ᐿॹ\n\r<\n�\\",ॹ✅,\n"✅ᐿ\raa\n\n\t;�π<,",ᐿ<ᐿ\\ᐿ✅;\t<π""ॹ<\t\n,,π"\t�✅\r;;ॹ,ᐿ\tॹ✅ॹ,\nᐿ\n\\;\n\nπa<✅aπ\t;✅\r;�✅;a\t\n✅ππ\t\rॹ\n\\<✅✅<\r✅\t\r✅<π\n;\n""��ॹ\r,ᐿ\naॹᐿπ\\π\n\n,�\t<�a\nπ\\✅,πॹ",πᐿa,ᐿᐿa\r<\ta\t\nॹ\n\r✅\r;πa✅�\t�π",πa\n<✅"a\t�\r<ᐿa,\naᐿॹᐿ\rॹᐿa�"\\a✅""✅✅ᐿ\t"ॹ<\n"\nπ✅✅\n\\ॹ\t;ᐿ"a,ॹ"aॹᐿᐿ\n,\\\nॹॹ,;ॹ;\\\n\n\t\n,\naॹπ\r"\n\t✅ॹ\r\tᐿ✅;\r\ta�ᐿ\t\t\nπᐿ<ॹ\tᐿ\na\nᐿ\tππ<<π\t✅;\r"ॹ\n\na�<ᐿ\r\nᐿᐿ";a<\rॹ\\πॹ\tᐿ\n\nᐿπ;\t;;✅ᐿ✅✅<�ॹ;\tᐿ,,\t,π✅\nᐿॹ;\r"ᐿa,ᐿᐿᐿa\nॹ<aॹ\r,;π<<\nπa�\n\\\r,ॹπ�\n"ᐿππ\n✅ॹπ\ra,πॹ\n\t<;πᐿᐿ✅ॹ\t�a✅\r�"a,π��,ॹa\n\\\rॹ\nॹ"π"π\ta�<π�\r;a,a\r<πᐿ\na<\r\t\nॹ\\\\\n\\<�\\�aπa;\r\\,,\nॹ"✅;"\n✅ᐿ✅a\n<ππ\tπ<a\t�\\<"✅\\\nᐿ;\r\t✅�✅π\r\r\r\n",ᐿ;ॹ\nᐿ\r"\naa"\n\t<,✅<a\\\n"ॹᐿ\n\\\t\t\r"ॹ<,,π�"ॹ<ॹ,\\ᐿ<\\π"\\<ᐿ\n\rॹ\na\t\nπ;\\π\\✅\r,\r�",,;;,<\n\t"\\\r\r"ॹ✅ॹ�\n�✅�\n�\\\n\n\rᐿॹ\tπa<;;\n\n�a\n\\ॹॹ\t;\n<\t\\<ॹॹ�✅π\t"<\n\tᐿπᐿ""�;\t;ॹ<π<✅\nππ<"\rॹ�πᐿ�\rπ,<,<ᐿ<;;�,\t<<ᐿ\t<\tॹ,π,<\\a\t;\n\r�a✅\r\r\t\nᐿᐿ✅\r;\n;�;\r✅\n,;✅ᐿ,\\\n<,<\\\t\n;<\\aᐿ\r,\n;\\\r�\rॹ\\\t";\t�;\n,\ta✅\r\t\r,\n\\\t\nᐿ✅\\"\naᐿ\\\\";\r<�✅;aॹ\t\t\\\t\tᐿ�\r,"\n"\taᐿ\na\rππॹ\nπᐿ\rॹ\nॹ\tπ,π\r<"\n,�\na;�\\✅,<""�\nππ\t\nπaॹaa✅\\;\\a\r\rᐿ,\\�;\\ᐿᐿ<a"\r;π"\\ᐿπ\tπॹ;\\a\ra;\t\n\\�\r<\t<ॹ\r✅\na\t\t<\n\n✅π\nᐿ✅\\<,\nπ\rᐿ✅a"\r"\n✅ॹ\\�\r\n\\�\nॹ\\<\\<\n\n\n	\\xc00913e02a63e4cf532d9b2ce282fad85af699815c18c595ea804462a794f75123135cc728c43daa5aa248d5a17dde4906843049a995cbd0b80d23694897467e10e43cb2ef4662aceef9304835d744e43af04165e3d13cd1f7b9adead9e072bca6ae92fbb52cc2f48f89ae44e020c506fa047bf7e8bf5a51a3054dbba8bef4c40fe97c737b7c8f72adda57c4a29edf179f02f3d0bfd3eafd8e7733b84037ea87984e250ae0406a71b3b02679e34b30c8bc5f731e1598e7bf36ebef7d2464642faaf1cb2e558fd472
106	ॹॹ;,✅\nπ<\t\nπ\tॹπ✅a\n,\nᐿ�\nॹ✅�ॹ�"✅ॹ\\<"\n;a\\\n,✅π\n<\n<\nॹπᐿ�ᐿ;,�\tᐿ\nᐿaᐿ,\nπ�\t\\<ॹ\\π;π�π"<;"�\\<�,<�\\a�<\nॹaᐿaॹ�\\ᐿπ,✅ᐿ"<✅✅a\t�ॹ\t<π;�ॹ\\ᐿ;✅\r\\,;\\ᐿॹ\nॹᐿππ\nᐿ\nᐿaπ\\\nπ\r"✅�π\nπ\rॹπ"ॹ✅a\ra�✅\nπ;ॹ✅\n;ॹ,�\nπ\rπᐿa\\\\ᐿ,π<ᐿ✅\n�,\r\nᐿ✅\n<�ᐿ""✅,,"\n<\n✅\rπa�π\n<\\ॹ\nॹ�;\ra��✅ᐿ\n,\t�;,π<\r��\r\\�\n✅\r✅�;\\\n\n,\nॹ✅π\n,\n✅\t,�<\nπ\t;aπ\n<a<\n\tπ\r"\\✅\n\n\n<ᐿπaπ\\�"<✅\\a,✅\n✅\n<""\n\n\r\rᐿ�\\\tᐿᐿ;\n\rᐿa\\π<\n\\\n\n";\r\r\raπ"\r�ॹa\r�"\n"✅ππ✅�\t�ᐿ\tᐿ\\\r�ᐿ<\\\nᐿπ✅\tॹ<π\ta"✅\t,ॹa✅ᐿ;\\\r✅\\,ॹ"a\n<ॹ\\\n<"π\\\\ᐿ\n✅\nᐿ\n,\n\r\t\n\r\n<aᐿ;ᐿ;ᐿ\r;✅a<a,,<\t\n\\ππ\\"✅\n\\a\n\tπa<\r<π\n✅\\π<ॹ,\t;<aaaπॹᐿaॹaॹ�,"\t,ॹ;\\<✅a\nᐿ"\nπ\\aᐿ�ᐿ<ॹ;\\<ᐿ\nᐿ\n"aᐿᐿπ,"\r✅ॹ\n,<\r<\n<<,ᐿᐿa,\rᐿ<;π\\�,"\rπ�\nππ�,✅;�\ra<;\r�ᐿ\tπ;"πᐿ\\�a"ᐿ\\;\\ॹ";ॹ;;✅\tॹ\r\n<\t\n\t<aॹ\tᐿ\n"ॹᐿ\t✅✅�ॹ;;<�\t,\n\r\n\n\ta"\\<\rπᐿa\t<\na;\t"\nπ<πॹ\r\n<\n✅ᐿॹ✅�<,;✅"\n<�π<✅<<✅\\;\n"\rᐿ\t�\n\n\r\t,ॹ�"\rᐿᐿᐿ,"π\nπ",a""<�\t\\πॹ\n\taॹᐿ\tπॹ,\n✅\rπa\r<<,\n\nᐿ;\t\\<\tᐿ\n\n�"ॹ<\n\r\nᐿ\n�\n\nπᐿ;\nॹ\n"π<"\r\r\n\r\\ᐿ;;πॹ;�\r✅�,✅\r\r�,a;ᐿ\\ॹ"\t\r✅;\t<,π,�\t"πaᐿ��\\ॹ"\n\tᐿ\t,ॹ✅�ᐿ✅\tᐿ,ॹ✅;;�\r\n✅ᐿ�\nπa;\\,✅ॹ<ᐿ\nπ\n"\n;a\t\\π\n<\r\r\rπ"\n\nᐿ<<ᐿ"\n,\n"ॹπaᐿπ��\r\n�ᐿॹ,\na\n\rॹ<�ॹ"\n\t\r\n,π\n�,<ॹ,<\n<�✅ᐿ\r✅a✅<\r;,�a�\\\nॹ<\\<✅ॹ"\nॹ\r�\ta;"\\ᐿ\n\n✅"\r;✅\t,a✅✅<"ᐿ\t�π\\✅✅�<;"✅π✅ॹ,\nπ\n��<,\ta\r�✅ᐿ\nॹπ\nᐿॹ✅;\nॹ\t\r\\\nᐿ�ᐿ\n\tᐿ,\r\\;<<a,"π,\tᐿ\nπ<a"ॹ\\aa\r\r"";\tᐿ,ॹa\nॹ\nπ	\\x158fe87f5cf3dff06e50dbd3e81b2242314ba86c2b962f6c0b9e0e91a04496c04af1a1cda0d2cc8305e3e23f4554e89b8200e5b01a8e135a75fdf281775eb7bd48b8e4079cfd997bb422e5519c178597f7c38b620fffb02d766d4fe1fbc3f3ab30a005751cb07b42ea6d70bd8f993f870fb265e3d8bf324c4e0317a79fd30f15240249d2d7045219339ddd587f0141ccde60506d3adb65fda607f86988a7eeacd3b8bf328479cd0ae65d4c29cad96078b41b7ce813821a282f2a2033604ae1d9b0e641644e212dd9
107	;,\t""\n\\<a<πa;\\;\t\\<\\a✅\t✅ॹ\t�✅\t\n\\<ॹ\n\tॹ\t\n✅\n,<\\<�\r"\t<ᐿ"πॹ�aॹॹ"�\nᐿπ\r✅ππππa�;�\t\t,\ra;ॹ\t\\,π✅\n"ᐿ";,�\t;\\"\raॹ\r"a<<\\✅<aॹ�<\r\nᐿ\t"ᐿ\t;",;\r\n�\t\ra<\nπ�\n"ᐿππ\t,�aa\n"\na\\�<\\ॹ;\n\r\\a<\tᐿππ<;π"ॹπ\tॹ"π;\n,\\\r✅a✅"<",\n\t\tᐿ✅ॹ\t<�aπ"ᐿ\\"a\r✅✅\na\r\tπᐿ✅�\\<\n��;a<\\;\nॹ\tπ�"\nπ\n"\\\n;�a,\\"\r\na;<\\\na\r\n,\\ॹ\r,\\aᐿॹ✅π,;;\\;\r\n\r<\r;;✅\\"\n\\\tॹ\r\n,\t;a"π�;\n\n"\t\n\tᐿ\n��\t✅π<✅�aॹ\t<\n�\\;,\\π\n""ॹa;�;\n\n\n;πaπ�ॹ\taॹ,\n"\t\n�\t\nᐿ\n\na;"ॹπ,\tॹ\n\\aᐿᐿ\n✅\rॹ<a\\\n"ᐿa;aa,ॹ\t\n""✅\\\t;ॹ;πॹॹ�,π;\n\rᐿ\n\nπ\n✅�\n�\n\t✅✅aॹ\n<",π✅ॹ�,a;\r\\,ᐿॹ<✅\nπᐿ"\n,π\r\nॹ<πaॹ<ᐿॹᐿ\t✅<aᐿᐿ\n\r"�ᐿᐿ"ॹ\r<\n\tॹ\n\t\nᐿ\n\\\r\tॹ��;ॹ"\\\\ᐿ✅\\a�✅"ॹ\tπ;a\r;;ᐿaπ;ᐿ\n\r\n\nᐿ;\\�ᐿ\t"a;"\r\\\r�\nॹa\r;ᐿᐿॹ✅a\r;",\r\nᐿ\nᐿॹ,\n\t�;π"a�,\n\nᐿ\r\\\n✅<\n\\,\n\t\tॹ,\n;<\n\t�ॹॹॹ✅"aॹπ\tᐿπ\n\naπ\nॹ,"\n"�\r�;\r;a✅\r\nᐿ\\\n\n�;;\t"a✅<,\n�\r,✅π\tᐿπ,\n�\naॹπ�\t�\n\r�a✅,,\t✅\t�a\n"a,,;ॹ✅\nॹ;\\�\r<\nᐿπ�\\;ॹ<\tᐿ\t"\\;ᐿ\nπ<\n\t;\\\r\\ॹॹ�,✅,ᐿ\n,π,�a;\rπ\\a✅\nॹ;\t,"ॹa;✅\n\t,\nॹ�ॹᐿ✅,"\naa✅\r\\\rॹa\rॹaaᐿ��\nπ\t\t✅\t��\na;✅\\";ᐿॹॹ\nᐿπ\r\n✅✅"""ॹ\tॹᐿ\n\\\ra<a✅\nπ"\r\\<,\t\rππ\n\nॹᐿ;�\rॹॹπππ\r;�ᐿaa"✅ॹ;\n\nॹ,✅\n<\nπ\nॹ\\\r\nᐿॹ\nॹ;π<\nπ✅a<,;\n\tπ✅\t\n\n\n\n";π�✅ॹॹᐿ\t,<a";✅a\t,\naᐿ\n\t,,\rॹ"πaᐿॹॹ\n✅\r\rᐿ<\t\t"ᐿॹa�\r\\;a<a\n\rᐿ<a,\t<\n✅\n<✅✅\n\\ᐿπ<ᐿ\ra\r\t<"�;ᐿ\t,\n,;\n✅"ᐿ\r<ॹ✅π✅\r\\ᐿπ"\rᐿ\n;ᐿ,✅<a\ta"a�;\n\\ॹᐿ"a�\t;"<π\r✅a\r✅ॹ,ॹ\r\\\\ॹᐿπ;�π<✅�\r\tॹ;ॹ	\\xf3ff4d451e429e182215aaee06a2d64b6d1aadc9e5031e4b99bf11ae0a796ebc44c85fd174bfccf43cb5f561cd0040e8566209385c6601ddb3fc1472b881d99c8428183c3fae7166ecbd7cc3ba26c55e2f5169c92f2f4691fae28d00f74ecaf24ca41c0d577477c4b2615a7b07d8dd0abb8f6026f0560c8be735092e6fd1322668df4d6b15c52422554e2ca75f2c30531c00dd92d4e7d5b2c494d150ad051371aed6b0eae1a7723c552f2a15b56f9129d9e3115d909e05ac788f4fdee8cf81def8e1fa903b13363e
108	\t"ᐿ;\nπ"�\\\n✅π\\\\\n\r\na\na<\n\tॹπ✅\t✅;a�✅,\n\r;\\\n;\n\t<\na,\n"<ᐿ\r\n\nᐿ\r\t\r�\n\r;<\n�<�<"\taπ,✅;\\�;\n;"\r�π�a✅\n�<\t,;✅ᐿπ\\\nॹ;a\t✅�","\n\t,"��\r�\\\r";π\n✅\r\ta✅ᐿᐿ\taa\n\n<\n✅"π\\,�;\n✅\n\n<,;<\na\rᐿ�;\n";,<✅aa<�"<a�\rᐿ\tॹᐿॹ\t",✅\n\t<\t\t�;,\n�\n�""\\✅<,,πॹ;aa",ॹ✅ᐿ";\nπ<"a\n,ॹ\nॹ\r;<<π\t\r;ᐿ;π\n✅π;ᐿa";\nॹ\nππ,ॹ\\\t�\n\ta�\t;a�<\t<ॹ;a;,ॹ\\aॹa\t"�π�\\,\\ᐿ\rᐿ\r\t�,\t,;✅✅<,\na;π\\<\t"\r\t,�\\�\n\\a\t\t�;π\n\n✅a,πᐿ;�;�,\t\r✅✅ᐿ\n\t\nॹ\\\n\n\r,\\✅ॹ\\\tπ\na\t<,\r\r,\n\\\n"✅\n\\ᐿ�<ᐿ�\n\tπ�ᐿ✅\t\r\n,\\π\\\\\\π,\t✅ᐿ✅\na,aa<�aa"\\\r",\raa",";;ॹ<a\\;\r<ᐿ,\n\\✅ᐿ"�"<�\n"\t\rॹ\n\n\t;ॹ\r✅✅a<\\",�\t\n\r\\�\\"πᐿᐿππ\n\tπ\r\nॹ\rॹ\nᐿ\\ॹ\t\n\t�\\"✅\ta��,✅<\r,<ॹ\n;ॹ\r<a,�,�✅✅,""ॹ\\<\rᐿ<;a\nπ\t\r;"π�\n;π;;ᐿ\r;;\t\n\n"ᐿॹa,πॹ\n✅\\ᐿ\nॹॹa<;aᐿ";ᐿ\\\t\t;aπ\\✅πॹ\r\r\\""ᐿππ\n,�\nπa\n,<;<✅✅a\n\nॹॹॹॹ,\tπ\n✅�\nᐿ"a"ᐿ<ᐿ,✅;\\;π\t<<\rॹ\n,\n<ᐿ\r;;✅ᐿॹॹॹ<ᐿᐿ"\rᐿa\t\\\\ॹ"\\\ra\r\n\t�\nॹᐿᐿ\ta\r,\ra✅ॹ\tᐿ\n\r<π\r\t\t\t\n�,\n,✅\t<<\r\n�"\t\n�ᐿॹ\ra,\n\rᐿॹ\n\t\t✅;<a\r,\nॹ,✅\n,,ᐿ\\✅a;;,�a,ॹॹπ<�<a\n"π\r�✅<,\\✅\r✅aa\tπ;\n\\<�\nॹ"<π";"�;\\,ᐿ"\n<,<ᐿa\n<;�\n\\,ππa"ॹॹ;\na"ॹa;",�\n\n\r;,ᐿ\nॹ\r✅π\r\t;π✅π\n;\r\ra,\t�a\n�ॹ<;�\\\n\n\r;a�\rᐿ;,\n�;;\ra�aπ;"\n\t<\n\\<ॹ<<;,;π\t\\\\ᐿ\n\n\t,<\na\n;\nᐿ\n\n\tᐿ<\tॹ"ᐿ\r\\\r�\\πॹ\n<<\ta\n\n✅\t\t\\\r,\n\\ᐿ\n\n✅"ॹ\n"ᐿ\nᐿa�π✅✅\r<\tᐿॹπ\\"\r✅✅a;"ᐿ\t\\<\rπ\\ᐿ\t,✅\\\t\nॹᐿ\t\t\r�ॹ\t<\\\n\tπॹ\r\t;a✅;�a�<,a\nπ\tᐿॹ;\\<a\t,aॹ\n\\\n\tᐿॹ<"��<a\r	\\x5079832da0a39e4f1cd6fdfc633c0fa6d58f41c06e609c1054e40bab03aeed1a6c42a5b424ae38421a8457d894a5593bd1da7f90fa45603d51ff78d8078998d0bbaa2890d02c28b17bcb212775664518ccc4630658ec76ecbc343c7b29929f404f17818a5d5af14a8b452513b5d751885f470b2fe8219c89539716581fe9a6373add13bc3f2c5e3962106a33388f8147f5a13a3727760e5274ad4e6d70599f2976bffa10e6ef87e4751c71c55bddc4d89110ef425700fcacfc228116dab11e390fe19cbd14cd1efc
109	✅,<ᐿ\r";\r\\\n\r\r\\�\nπॹ�\n\nॹ�ॹॹ<<�\nᐿ\t;ॹ\n�\n\t;ᐿ✅\\�"<"<\nᐿ,\t\n\n;ᐿ\\\r\tᐿ\n<ᐿॹ\r�<;","�\ra;a,\tπ\n<aa\\;"✅✅\n\t\\,,\r;�;"\n�ॹa\r�,,ππ\n"✅\\<�\nπॹᐿa\nᐿ<✅\rॹᐿ\n✅;\nᐿ\nॹॹ�<,\tπ,a\t✅\nॹ<\raπॹ\n\r\n\r<\tॹ<\raᐿπ;,πॹ\t\naॹ\\✅<\rॹa\t\t✅";\\\nॹ\r;\t✅;\n\ra\\\n<✅,a;\\\t\n✅"a\na�\rॹ<�"ॹ\n"�\r\n�ᐿπ��\nᐿ✅\n,ᐿ�;�\n\ta;""�ᐿ\r\\\n\n,πᐿ,ᐿ�ᐿ✅\r�a;\na"π,;\r"✅πᐿᐿ\n\\\t\\,<<ᐿ�\n✅ॹ,\r;\\�\na,;\r;,\\ᐿ\t\t\\"ॹ\t✅π<π;<<✅ॹ,"a\r\r<,;✅✅ॹπॹ"\r\\\ra\\"ॹ�ॹa"a�✅;✅<\t<π;�\n���π\n"<;a<✅\n\r"aa,✅;\t;\\"\r<\r,\n��ᐿ\n",\t;\n"ॹ✅;ॹ\n�a��a,πॹπᐿ\n�;ᐿa\t;ᐿ<\t\r�ᐿ<a\t;�,,ᐿ\n\n"\na"\n\nॹ✅\n\n"π\\a<✅\nπ\\\t"✅\r\n✅π\r\ta\\�\r\n\r\\\nॹᐿ✅\\\nᐿ�\r;✅π;\t;<\\a<�\\aᐿॹ\n✅"\nπ\tᐿ\n;ॹॹ"ॹπ�a<\n\\,✅\r\nπ\\aॹ\n,\\ᐿa,,ᐿ<ᐿ,✅a<\n,\na\r,\na,ᐿ<ॹaπ""✅,✅π✅\tॹ;\n\r"\n"ॹ\t<✅"\nπ\nππ�\\ॹa\\\\�\tॹ;,π\t,;,ॹaπ\r�\n\na�\\ॹ✅π"\t�a\\\r,�;\n\n\nᐿᐿ\n"ᐿ\\\n,\n\nॹ<ॹ\na"aᐿ\\\\✅�\rॹ\tॹ;\r\n"✅\t<✅ॹ�\t✅,\tᐿ\na\n;a\nᐿ;ॹ\t;π\n\tᐿॹ"�;\\\r\t"\n,;\n\t""aᐿ\tॹ�a\\aa\n<✅"a✅\t\\ॹ��"<"a,�✅\\ॹ\n;\r,<\\\rᐿ"\rᐿ<\n✅\n\n;✅\r✅ॹᐿ\\\t"π<\n✅π;πᐿ\rॹ\n"\r\t\nॹ;\nπᐿa✅\t✅�\n"\r<a✅�\r\\\n�\n\n"\n\r\t\n,;\r�π\n�\\ॹ\r;\n,\ra\\"✅;ॹ\\"π✅\t\\π✅ॹ\t,\\ππ\r\\ᐿ""a\nॹπ;\t<aॹॹ;✅\t\t\n�\t\\\n,\n\raॹ"π,\n\n\\<a;\rॹ\t\nᐿπᐿॹaπᐿॹ\n\n,\r;,"✅,\r\t,\n✅\tπ"\nᐿπa\n,\n\n;"\n\nπᐿ�"<,;;�aᐿ;,\\a<ᐿ,✅�<;ᐿ✅✅ॹ"\nπ\n,\n\na"ॹ<<"\n✅ᐿ✅�\\aॹ\\✅\n\rᐿ;\n<\n\n�\n;\n\\ᐿaᐿ<<a�\na\r\n,ॹa\t,;"<,\r;<"\n"<✅ᐿ\n<aᐿ✅\n\n\\ॹπ\n,\n\\\n\t;�π�<aᐿ\\\t"\n\n	\\xa6fe18955d8177702fc91d5829f4464feef626abe35e0a4ff22575ba7dc2e583259865b82a6b975a76c3aa8119b3a3a72c369836771e1bc52f01797838894c0ff61f190400e00fc0acfad9abd67e485f7470aa7f841f6d18f9d8eba9234daa2071283413e05c0d8be8197e6c201eda08f413e610bf947684ec3b465f40fdc9869ee29c5b74ed20bfb692ff8518faf01335fdbc5c21c35e8f51f4371f48c3b34b84291599ec1fe67db0e51e084756f5c81d7cbff7d1a71e15f5a5ae0a5ea6d721e5e442ed09bb961a
\.


--
-- Name: simple simple_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.simple
    ADD CONSTRAINT simple_pkey PRIMARY KEY (i);


--
-- Name: simple_b_s_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX simple_b_s_idx ON public.simple USING btree (b, s);


--
-- Name: simple_s_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX simple_s_idx ON public.simple USING btree (s);


--
-- PostgreSQL database dump complete
--

