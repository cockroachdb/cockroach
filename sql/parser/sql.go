//line sql.y:6
package parser

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*tokenizer).parseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
	yylex.(*tokenizer).allowComments = allow
}

func forceEOF(yylex interface{}) {
	yylex.(*tokenizer).forceEOF = true
}

//line sql.y:24
type yySymType struct {
	yys         int
	empty       struct{}
	statement   Statement
	selStmt     SelectStatement
	byt         byte
	str         string
	str2        []string
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	smTableExpr SimpleTableExpr
	tableName   *TableName
	indexHints  *IndexHints
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	tuple       Tuple
	valExprs    ValExprs
	values      Values
	subquery    *Subquery
	caseExpr    *CaseExpr
	whens       []*When
	when        *When
	orderBy     OrderBy
	order       *Order
	limit       *Limit
	insRows     InsertRows
	updateExprs UpdateExprs
	updateExpr  *UpdateExpr
	tableDefs   TableDefs
	tableDef    TableDef
}

const tokLexError = 57346
const tokSelect = 57347
const tokInsert = 57348
const tokUpdate = 57349
const tokDelete = 57350
const tokFrom = 57351
const tokWhere = 57352
const tokGroup = 57353
const tokHaving = 57354
const tokOrder = 57355
const tokBy = 57356
const tokLimit = 57357
const tokOffset = 57358
const tokFor = 57359
const tokAll = 57360
const tokDistinct = 57361
const tokAs = 57362
const tokExists = 57363
const tokIn = 57364
const tokIs = 57365
const tokLike = 57366
const tokBetween = 57367
const tokNull = 57368
const tokAsc = 57369
const tokDesc = 57370
const tokValues = 57371
const tokInto = 57372
const tokDuplicate = 57373
const tokKey = 57374
const tokDefault = 57375
const tokSet = 57376
const tokLock = 57377
const tokInt = 57378
const tokTinyInt = 57379
const tokSmallInt = 57380
const tokMediumInt = 57381
const tokBigInt = 57382
const tokInteger = 57383
const tokReal = 57384
const tokDouble = 57385
const tokFloat = 57386
const tokDecimal = 57387
const tokNumeric = 57388
const tokDate = 57389
const tokTime = 57390
const tokDateTime = 57391
const tokTimestamp = 57392
const tokChar = 57393
const tokVarChar = 57394
const tokBinary = 57395
const tokVarBinary = 57396
const tokText = 57397
const tokTinyText = 57398
const tokMediumText = 57399
const tokLongText = 57400
const tokBlob = 57401
const tokTinyBlob = 57402
const tokMediumBlob = 57403
const tokLongBlob = 57404
const tokBit = 57405
const tokEnum = 57406
const tokID = 57407
const tokString = 57408
const tokNumber = 57409
const tokValueArg = 57410
const tokComment = 57411
const tokLE = 57412
const tokGE = 57413
const tokNE = 57414
const tokNullSafeEqual = 57415
const tokUnion = 57416
const tokMinus = 57417
const tokExcept = 57418
const tokIntersect = 57419
const tokJoin = 57420
const tokStraightJoin = 57421
const tokLeft = 57422
const tokRight = 57423
const tokInner = 57424
const tokOuter = 57425
const tokCross = 57426
const tokNatural = 57427
const tokUse = 57428
const tokForce = 57429
const tokOn = 57430
const tokUsing = 57431
const tokAnd = 57432
const tokOr = 57433
const tokNot = 57434
const tokUnary = 57435
const tokCase = 57436
const tokWhen = 57437
const tokThen = 57438
const tokElse = 57439
const tokEnd = 57440
const tokCreate = 57441
const tokAlter = 57442
const tokDrop = 57443
const tokRename = 57444
const tokTruncate = 57445
const tokShow = 57446
const tokDatabase = 57447
const tokDatabases = 57448
const tokTable = 57449
const tokTables = 57450
const tokIndex = 57451
const tokView = 57452
const tokColumns = 57453
const tokFull = 57454
const tokTo = 57455
const tokIgnore = 57456
const tokIf = 57457
const tokUnique = 57458
const tokUnsigned = 57459
const tokPrimary = 57460

var yyToknames = []string{
	"tokLexError",
	"tokSelect",
	"tokInsert",
	"tokUpdate",
	"tokDelete",
	"tokFrom",
	"tokWhere",
	"tokGroup",
	"tokHaving",
	"tokOrder",
	"tokBy",
	"tokLimit",
	"tokOffset",
	"tokFor",
	"tokAll",
	"tokDistinct",
	"tokAs",
	"tokExists",
	"tokIn",
	"tokIs",
	"tokLike",
	"tokBetween",
	"tokNull",
	"tokAsc",
	"tokDesc",
	"tokValues",
	"tokInto",
	"tokDuplicate",
	"tokKey",
	"tokDefault",
	"tokSet",
	"tokLock",
	"tokInt",
	"tokTinyInt",
	"tokSmallInt",
	"tokMediumInt",
	"tokBigInt",
	"tokInteger",
	"tokReal",
	"tokDouble",
	"tokFloat",
	"tokDecimal",
	"tokNumeric",
	"tokDate",
	"tokTime",
	"tokDateTime",
	"tokTimestamp",
	"tokChar",
	"tokVarChar",
	"tokBinary",
	"tokVarBinary",
	"tokText",
	"tokTinyText",
	"tokMediumText",
	"tokLongText",
	"tokBlob",
	"tokTinyBlob",
	"tokMediumBlob",
	"tokLongBlob",
	"tokBit",
	"tokEnum",
	"tokID",
	"tokString",
	"tokNumber",
	"tokValueArg",
	"tokComment",
	"tokLE",
	"tokGE",
	"tokNE",
	"tokNullSafeEqual",
	"'('",
	"'='",
	"'<'",
	"'>'",
	"'~'",
	"tokUnion",
	"tokMinus",
	"tokExcept",
	"tokIntersect",
	"','",
	"tokJoin",
	"tokStraightJoin",
	"tokLeft",
	"tokRight",
	"tokInner",
	"tokOuter",
	"tokCross",
	"tokNatural",
	"tokUse",
	"tokForce",
	"tokOn",
	"tokUsing",
	"tokAnd",
	"tokOr",
	"tokNot",
	"'&'",
	"'|'",
	"'^'",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'.'",
	"tokUnary",
	"tokCase",
	"tokWhen",
	"tokThen",
	"tokElse",
	"tokEnd",
	"tokCreate",
	"tokAlter",
	"tokDrop",
	"tokRename",
	"tokTruncate",
	"tokShow",
	"tokDatabase",
	"tokDatabases",
	"tokTable",
	"tokTables",
	"tokIndex",
	"tokView",
	"tokColumns",
	"tokFull",
	"tokTo",
	"tokIgnore",
	"tokIf",
	"tokUnique",
	"tokUnsigned",
	"tokPrimary",
}
var yyStatenames = []string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = []int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyNprod = 261
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 690

var yyAct = []int{

	107, 380, 311, 179, 450, 267, 67, 98, 104, 260,
	177, 263, 399, 217, 105, 387, 395, 137, 303, 254,
	195, 473, 473, 93, 180, 3, 28, 29, 30, 31,
	494, 94, 153, 154, 473, 309, 473, 473, 70, 72,
	473, 432, 483, 148, 315, 309, 69, 82, 436, 115,
	148, 85, 58, 148, 246, 468, 90, 278, 279, 280,
	281, 282, 437, 283, 284, 89, 419, 80, 274, 73,
	244, 99, 493, 492, 362, 364, 130, 131, 132, 68,
	134, 247, 136, 141, 138, 491, 490, 486, 474, 142,
	78, 472, 145, 146, 370, 314, 308, 150, 418, 417,
	347, 298, 50, 140, 296, 245, 51, 366, 81, 176,
	178, 363, 84, 181, 53, 52, 47, 182, 44, 371,
	255, 46, 153, 154, 255, 70, 301, 48, 70, 289,
	199, 193, 189, 69, 200, 48, 69, 373, 152, 198,
	433, 39, 431, 40, 41, 213, 42, 43, 166, 167,
	168, 73, 223, 199, 164, 165, 166, 167, 168, 99,
	186, 225, 226, 214, 215, 129, 227, 125, 385, 232,
	233, 228, 236, 237, 238, 239, 240, 241, 242, 243,
	222, 234, 135, 15, 16, 17, 18, 153, 154, 204,
	224, 304, 70, 70, 248, 99, 99, 221, 259, 210,
	69, 261, 268, 270, 265, 271, 127, 197, 250, 252,
	428, 429, 19, 144, 262, 258, 57, 304, 54, 208,
	55, 56, 368, 272, 127, 161, 162, 163, 164, 165,
	166, 167, 168, 288, 413, 414, 416, 275, 356, 246,
	386, 291, 292, 357, 290, 354, 28, 29, 30, 31,
	355, 211, 415, 235, 360, 359, 358, 295, 196, 471,
	196, 461, 99, 302, 469, 426, 147, 375, 220, 221,
	20, 348, 306, 300, 310, 297, 128, 219, 307, 191,
	460, 459, 161, 162, 163, 164, 165, 166, 167, 168,
	352, 353, 22, 23, 26, 24, 25, 21, 251, 15,
	110, 207, 209, 206, 369, 114, 458, 453, 120, 183,
	70, 407, 372, 377, 406, 388, 268, 110, 376, 192,
	400, 383, 114, 378, 381, 120, 221, 221, 396, 201,
	187, 276, 382, 127, 389, 390, 391, 392, 393, 394,
	148, 401, 397, 398, 97, 111, 112, 113, 408, 402,
	403, 404, 405, 102, 185, 184, 122, 118, 287, 220,
	64, 97, 111, 112, 113, 151, 409, 73, 219, 71,
	102, 367, 365, 349, 118, 212, 194, 101, 139, 420,
	65, 116, 117, 95, 421, 87, 86, 124, 121, 435,
	423, 467, 466, 488, 101, 88, 422, 444, 116, 117,
	95, 448, 374, 286, 63, 121, 15, 451, 451, 119,
	73, 489, 452, 249, 434, 229, 456, 230, 231, 455,
	294, 457, 454, 248, 123, 425, 119, 126, 465, 79,
	257, 202, 269, 496, 143, 463, 464, 381, 61, 438,
	439, 440, 441, 442, 443, 59, 445, 446, 447, 312,
	449, 264, 91, 92, 451, 45, 412, 476, 475, 70,
	451, 451, 451, 70, 478, 479, 480, 261, 477, 481,
	482, 69, 484, 110, 485, 15, 313, 83, 114, 293,
	15, 120, 161, 162, 163, 164, 165, 166, 167, 168,
	411, 110, 424, 351, 196, 495, 114, 497, 498, 120,
	133, 114, 77, 76, 120, 75, 66, 462, 161, 162,
	163, 164, 165, 166, 167, 168, 15, 71, 111, 112,
	113, 278, 279, 280, 281, 282, 102, 283, 284, 33,
	118, 430, 384, 316, 266, 71, 111, 112, 113, 470,
	71, 111, 112, 113, 102, 74, 203, 273, 118, 183,
	101, 205, 49, 118, 116, 117, 114, 190, 487, 120,
	427, 121, 379, 410, 156, 160, 158, 159, 101, 350,
	299, 188, 116, 117, 253, 109, 106, 116, 117, 121,
	108, 305, 119, 103, 121, 161, 162, 163, 164, 165,
	166, 167, 168, 256, 155, 71, 111, 112, 113, 100,
	119, 361, 218, 32, 183, 119, 277, 216, 118, 96,
	285, 149, 172, 173, 174, 175, 60, 169, 170, 171,
	34, 35, 36, 37, 38, 27, 62, 14, 13, 12,
	11, 10, 116, 117, 9, 8, 7, 6, 5, 121,
	157, 161, 162, 163, 164, 165, 166, 167, 168, 4,
	2, 1, 0, 0, 0, 0, 0, 0, 0, 346,
	119, 318, 319, 320, 321, 322, 323, 324, 325, 326,
	327, 328, 329, 330, 331, 332, 333, 334, 335, 336,
	337, 338, 339, 340, 341, 342, 343, 344, 317, 345,
}
var yyPact = []int{

	178, -1000, -1000, 167, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 20, -4, -23, -7, -8, 96, 511, 427, -1000,
	-1000, -1000, 419, -1000, 374, 315, 497, 304, 302, -1000,
	496, 494, 493, -36, -63, -16, 302, -63, -1000, -10,
	302, -1000, 321, 320, -65, 302, -65, -65, -1000, -1000,
	296, -1000, 287, 315, 353, 60, 315, 141, -1000, 201,
	-1000, 58, -1000, -1000, -1000, 302, 302, 302, 491, 302,
	84, 302, -1000, 302, 313, -1000, -45, -1000, 302, 413,
	119, 302, 302, 257, -1000, -1000, 345, 31, 91, 542,
	-1000, 452, 470, -1000, -1000, -1000, 530, 281, 280, -1000,
	256, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 530, -1000, 245, 304, 311, 484, 304, 530, 302,
	-1000, -1000, -1000, 302, 255, 410, 94, -1000, -1000, 186,
	-1000, 310, -1000, -1000, 302, -1000, -1000, 203, 296, -1000,
	-1000, 302, 86, 452, 452, 530, 235, 393, 530, 530,
	155, 530, 530, 530, 530, 530, 530, 530, 530, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 542, -64, -29,
	-53, 542, -1000, 475, 279, 296, -1000, 511, 10, 486,
	401, 304, 304, 250, -1000, 438, 452, -1000, 486, -1000,
	-1000, 4, -1000, 109, 302, -1000, -60, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 248, 437, 338, 294,
	22, -1000, -1000, -1000, -1000, -1000, -1000, 486, -1000, 235,
	530, 530, 486, 383, -1000, 394, 52, 52, 52, 44,
	44, -1000, -1000, -1000, -1000, -1000, 530, -1000, 486, -1000,
	-30, 296, -33, 14, -1000, 452, 97, 235, 167, 123,
	-38, -1000, 438, 434, 462, 91, -39, -1000, 625, -24,
	302, -1000, -1000, 308, -1000, 482, 203, 203, -1000, -1000,
	161, 154, 172, 171, 170, -18, -1000, 307, -27, 306,
	-1000, 486, 126, 530, -1000, 486, -1000, -40, -1000, 6,
	-1000, 530, 26, -1000, 371, 184, -1000, -1000, -1000, 304,
	434, -1000, 530, 530, -1000, 4, 142, 241, 241, 241,
	241, 241, 241, 241, 254, 254, 254, 246, 246, -1000,
	-1000, -1000, -1000, 241, 241, 241, 241, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 240, 237, 302, -1000, -1000,
	478, 442, 437, 140, -1000, 168, -1000, 152, -1000, -1000,
	-1000, -1000, -25, -26, -58, -1000, -1000, -1000, 530, 486,
	-1000, -1000, 486, 530, 364, 235, -1000, -1000, 409, 182,
	-1000, 183, -1000, -1000, 9, -1000, 388, -1000, 302, -70,
	-70, -70, -70, -70, -70, -70, 302, -70, -70, -70,
	302, -70, -1000, -1000, -1000, -1000, 302, 302, 233, -1000,
	438, 452, 530, 452, 232, -1000, -1000, 207, 206, 187,
	486, 486, 500, -1000, 530, 530, 530, -1000, -1000, -1000,
	-1000, 360, -1000, 359, -1000, -79, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 181, -1000, -1000, -1000, 176, -1000,
	-43, -1000, -46, 302, 434, 91, 156, 91, 304, 302,
	302, 302, 304, 486, 486, -1000, -1000, -1000, -1000, 302,
	-92, 302, -1000, 302, -1000, -47, 376, -48, -49, -61,
	-62, 141, -104, -1000, -1000, -1000, -1000, -1000, 488, 411,
	-1000, -1000, -1000, -1000, -1000, -1000, 302, 302, -1000,
}
var yyPgo = []int{

	0, 651, 650, 24, 649, 638, 637, 636, 635, 634,
	631, 630, 629, 628, 627, 603, 626, 625, 616, 23,
	31, 611, 610, 609, 607, 13, 606, 602, 360, 601,
	4, 20, 7, 599, 594, 593, 583, 10, 14, 3,
	581, 580, 49, 576, 8, 575, 574, 19, 571, 570,
	569, 563, 11, 562, 1, 560, 2, 558, 557, 9,
	18, 6, 79, 395, 552, 551, 547, 546, 48, 432,
	429, 545, 15, 16, 12, 539, 0, 534, 5, 533,
	532, 531, 17, 529,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 3, 3, 4, 4, 5, 6,
	7, 8, 9, 9, 9, 9, 9, 10, 10, 10,
	10, 77, 77, 78, 78, 79, 79, 79, 79, 79,
	79, 79, 79, 79, 79, 79, 79, 79, 79, 79,
	79, 79, 79, 79, 79, 79, 79, 79, 79, 79,
	79, 79, 79, 79, 79, 80, 80, 80, 81, 81,
	81, 81, 81, 11, 11, 11, 12, 13, 14, 14,
	14, 14, 83, 15, 16, 16, 17, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 23, 23,
	21, 21, 21, 24, 24, 25, 25, 25, 25, 25,
	22, 22, 22, 26, 26, 26, 26, 26, 26, 26,
	26, 26, 27, 27, 27, 28, 28, 29, 29, 29,
	29, 30, 30, 31, 31, 32, 32, 32, 32, 32,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	34, 34, 34, 34, 34, 34, 34, 35, 35, 40,
	40, 38, 38, 42, 39, 39, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 41, 41, 43, 43, 43, 45, 48,
	48, 46, 46, 47, 49, 49, 44, 44, 36, 36,
	36, 36, 50, 50, 51, 51, 52, 52, 53, 53,
	54, 55, 55, 55, 56, 56, 56, 56, 57, 57,
	57, 58, 58, 59, 59, 60, 60, 61, 61, 62,
	63, 63, 70, 70, 64, 64, 65, 65, 65, 65,
	65, 66, 66, 69, 69, 72, 72, 73, 73, 74,
	74, 75, 75, 68, 68, 67, 67, 71, 71, 76,
	82,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 12, 3, 7, 7, 8, 7,
	3, 3, 2, 3, 4, 4, 5, 8, 8, 4,
	4, 1, 3, 4, 6, 2, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 1, 1, 1,
	1, 2, 2, 2, 2, 1, 1, 1, 1, 1,
	1, 1, 1, 4, 4, 0, 1, 2, 0, 2,
	1, 1, 2, 6, 7, 4, 5, 3, 4, 5,
	5, 5, 0, 2, 0, 2, 1, 2, 1, 1,
	1, 0, 1, 1, 3, 1, 2, 3, 1, 1,
	0, 1, 2, 1, 3, 3, 3, 3, 5, 7,
	0, 1, 2, 1, 1, 2, 3, 2, 3, 2,
	2, 2, 1, 3, 1, 1, 3, 0, 5, 5,
	5, 1, 3, 0, 2, 1, 3, 3, 2, 3,
	3, 3, 4, 3, 4, 5, 6, 3, 4, 2,
	1, 1, 1, 1, 1, 1, 1, 2, 1, 1,
	3, 3, 1, 3, 1, 3, 1, 1, 1, 3,
	3, 3, 3, 3, 3, 3, 3, 2, 3, 4,
	5, 4, 1, 1, 1, 1, 1, 1, 5, 0,
	1, 1, 2, 4, 0, 2, 1, 3, 1, 1,
	1, 1, 0, 3, 0, 2, 0, 3, 1, 3,
	2, 0, 1, 1, 0, 2, 4, 4, 0, 2,
	4, 0, 3, 1, 3, 0, 5, 1, 3, 3,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 3, 0, 5, 0,
	4, 0, 2, 0, 1, 0, 2, 0, 2, 1,
	0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, -14, 5, 6, 7, 8, 34,
	92, 119, 114, 115, 117, 118, 116, -17, 79, 80,
	81, 82, -15, -83, -15, -15, -15, -15, -15, 121,
	123, 124, 126, 127, 122, -69, 125, 120, 131, -64,
	125, 129, 122, 122, 122, 124, 125, 120, -3, 18,
	-18, 19, -16, 30, -28, 65, 9, -61, -62, -44,
	-76, 65, -76, 65, -71, 9, 9, 9, 126, -70,
	130, 124, -76, -70, 122, -76, 65, 65, -63, 130,
	-76, -63, -63, -19, -20, 104, -23, 65, -32, -37,
	-33, 98, 74, -36, -44, -38, -43, -76, -41, -45,
	21, 66, 67, 68, 26, -42, 102, 103, 78, 130,
	29, 109, 69, -28, 34, 107, -28, 83, 75, 107,
	-76, -76, -76, 9, -76, 98, -76, -82, -76, 65,
	-82, 128, -76, 21, 94, -76, -76, 9, 83, -21,
	-76, 20, 107, 96, 97, -34, 22, 98, 24, 25,
	23, 99, 100, 101, 102, 103, 104, 105, 106, 75,
	76, 77, 70, 71, 72, 73, -32, -37, -32, -39,
	-3, -37, -37, 74, 74, 74, -42, 74, -48, -37,
	-58, 34, 74, -61, 65, -31, 10, -62, -37, -76,
	-76, 74, 21, -67, 95, -65, 117, 115, 33, 116,
	13, 65, 65, -76, -82, -82, -24, -25, -27, 74,
	65, -42, -20, -76, 104, -32, -32, -37, -38, 22,
	24, 25, -37, -37, 26, 98, -37, -37, -37, -37,
	-37, -37, -37, -37, 134, 134, 83, 134, -37, 134,
	-19, 19, -19, -46, -47, 110, -35, 29, -3, -61,
	-59, -44, -31, -52, 13, -32, -77, -78, -76, -69,
	94, -76, -82, -66, 128, -31, 83, -26, 84, 85,
	86, 87, 88, 90, 91, -22, 65, 20, -25, 107,
	-38, -37, -37, 96, 26, -37, 134, -19, 134, -49,
	-47, 112, -32, -60, 94, -40, -38, -60, 134, 83,
	-52, -56, 15, 14, 134, 83, -79, 63, 36, 37,
	38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
	48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 64, 34, 124, -76, 65,
	-50, 11, -25, -25, 84, 89, 84, 89, 84, 84,
	84, -29, 92, 129, 93, 65, 134, 65, 96, -37,
	134, 113, -37, 111, 31, 83, -44, -56, -37, -53,
	-54, -37, -82, -78, -80, 26, 98, -72, 74, -72,
	-72, -72, -72, -72, -72, -73, 74, -73, -73, -74,
	74, -74, -72, -72, -72, -72, 74, 74, -76, -82,
	-51, 12, 14, 94, 95, 84, 84, 124, 124, 124,
	-37, -37, 32, -38, 83, 16, 83, -55, 27, 28,
	-81, 133, 32, 131, 26, -76, -68, 132, -68, -68,
	-68, -68, -68, -68, -76, -68, -68, -68, -76, -68,
	-30, -76, -30, 74, -52, -32, -39, -32, 74, 74,
	74, 74, 7, -37, -37, -54, 32, 32, 134, 83,
	-75, 83, 134, 83, 134, -30, -56, -59, -30, -30,
	-30, -61, -76, 134, -76, -76, 134, -57, 17, 35,
	134, 134, 134, 134, 134, 7, 22, -76, -76,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 82, 82, 82, 82, 82,
	82, 0, 243, 234, 0, 0, 0, 0, 86, 88,
	89, 90, 91, 84, 0, 0, 0, 0, 0, 22,
	257, 0, 0, 0, 232, 0, 0, 232, 244, 0,
	0, 235, 0, 0, 230, 0, 230, 230, 15, 87,
	0, 92, 83, 0, 0, 125, 0, 20, 227, 0,
	196, 259, 21, 259, 23, 0, 0, 0, 0, 0,
	0, 0, 260, 0, 0, 260, 0, 77, 0, 0,
	0, 0, 0, 0, 93, 95, 100, 259, 98, 99,
	135, 0, 0, 166, 167, 168, 0, 196, 0, 182,
	0, 198, 199, 200, 201, 162, 185, 186, 187, 183,
	184, 189, 85, 221, 0, 0, 133, 0, 0, 0,
	258, 24, 25, 0, 0, 0, 255, 29, 30, 0,
	75, 0, 78, 231, 0, 260, 260, 0, 0, 96,
	101, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 150,
	151, 152, 153, 154, 155, 156, 138, 0, 0, 0,
	0, 164, 177, 0, 0, 0, 149, 0, 0, 190,
	0, 0, 0, 133, 126, 206, 0, 228, 229, 197,
	26, 243, 233, 0, 0, 260, 241, 236, 237, 238,
	239, 240, 76, 79, 80, 81, 133, 103, 110, 0,
	122, 124, 94, 102, 97, 136, 137, 140, 141, 0,
	0, 0, 143, 0, 147, 0, 169, 170, 171, 172,
	173, 174, 175, 176, 139, 161, 0, 163, 164, 178,
	0, 0, 0, 194, 191, 0, 225, 0, 158, 225,
	0, 223, 206, 214, 0, 134, 0, 31, 0, 0,
	0, 256, 73, 0, 242, 202, 0, 0, 113, 114,
	0, 0, 0, 0, 0, 127, 111, 0, 0, 0,
	142, 144, 0, 0, 148, 165, 179, 0, 181, 0,
	192, 0, 0, 16, 0, 157, 159, 17, 222, 0,
	214, 19, 0, 0, 260, 243, 65, 245, 245, 245,
	245, 245, 245, 245, 247, 247, 247, 249, 249, 47,
	48, 49, 50, 245, 245, 245, 245, 55, 56, 57,
	58, 59, 60, 61, 62, 0, 0, 0, 260, 74,
	204, 0, 104, 107, 115, 0, 117, 0, 119, 120,
	121, 105, 0, 0, 0, 112, 106, 123, 0, 145,
	180, 188, 195, 0, 0, 0, 224, 18, 215, 207,
	208, 211, 27, 32, 68, 66, 0, 35, 0, 253,
	253, 253, 253, 253, 253, 253, 0, 253, 253, 253,
	0, 253, 51, 52, 53, 54, 0, 0, 0, 28,
	206, 0, 0, 0, 0, 116, 118, 0, 0, 0,
	146, 193, 0, 160, 0, 0, 0, 210, 212, 213,
	33, 0, 70, 71, 67, 0, 36, 254, 37, 38,
	39, 40, 41, 42, 0, 43, 44, 45, 251, 46,
	0, 131, 0, 0, 214, 205, 203, 108, 0, 0,
	0, 0, 0, 216, 217, 209, 69, 72, 246, 0,
	0, 0, 63, 0, 64, 0, 218, 0, 0, 0,
	0, 226, 0, 250, 252, 132, 34, 14, 0, 0,
	109, 128, 129, 130, 248, 219, 0, 0, 220,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 106, 99, 3,
	74, 134, 104, 102, 83, 103, 107, 105, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	76, 75, 77, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 101, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 100, 3, 78,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
	62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 73, 79, 80, 81, 82, 84, 85, 86, 87,
	88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
	98, 108, 109, 110, 111, 112, 113, 114, 115, 116,
	117, 118, 119, 120, 121, 122, 123, 124, 125, 126,
	127, 128, 129, 130, 131, 132, 133,
}
var yyTok3 = []int{
	0,
}

//line yaccpar:1

/*	parser for yacc output	*/

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

const yyFlag = -1000

func yyTokname(c int) string {
	// 4 is TOKSTART above
	if c >= 4 && c-4 < len(yyToknames) {
		if yyToknames[c-4] != "" {
			return yyToknames[c-4]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yylex1(lex yyLexer, lval *yySymType) int {
	c := 0
	char := lex.Lex(lval)
	if char <= 0 {
		c = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		c = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			c = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		c = yyTok3[i+0]
		if c == char {
			c = yyTok3[i+1]
			goto out
		}
	}

out:
	if c == 0 {
		c = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(c), uint(char))
	}
	return c
}

func yyParse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
	}
	yyn += yychar
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yychar { /* valid shift */
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yychar < 0 {
			yychar = yylex1(yylex, &yylval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yychar {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error("syntax error")
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yychar))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}
			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		//line sql.y:156
		{
			setParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:162
		{
			yyVAL.statement = yyS[yypt-0].selStmt
		}
	case 3:
		yyVAL.statement = yyS[yypt-0].statement
	case 4:
		yyVAL.statement = yyS[yypt-0].statement
	case 5:
		yyVAL.statement = yyS[yypt-0].statement
	case 6:
		yyVAL.statement = yyS[yypt-0].statement
	case 7:
		yyVAL.statement = yyS[yypt-0].statement
	case 8:
		yyVAL.statement = yyS[yypt-0].statement
	case 9:
		yyVAL.statement = yyS[yypt-0].statement
	case 10:
		yyVAL.statement = yyS[yypt-0].statement
	case 11:
		yyVAL.statement = yyS[yypt-0].statement
	case 12:
		yyVAL.statement = yyS[yypt-0].statement
	case 13:
		yyVAL.statement = yyS[yypt-0].statement
	case 14:
		//line sql.y:179
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].str2), Distinct: yyS[yypt-9].str, Exprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(astWhere, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(astHaving, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 15:
		//line sql.y:183
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 16:
		//line sql.y:189
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 17:
		//line sql.y:193
		{
			cols := make(Columns, 0, len(yyS[yypt-1].updateExprs))
			vals := make(ValTuple, 0, len(yyS[yypt-1].updateExprs))
			for _, col := range yyS[yypt-1].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 18:
		//line sql.y:205
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].str2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:211
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 20:
		//line sql.y:217
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].str2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 21:
		//line sql.y:223
		{
			yyVAL.statement = &Use{Comments: Comments(yyS[yypt-1].str2), Name: yyS[yypt-0].str}
		}
	case 22:
		//line sql.y:229
		{
			yyVAL.statement = &DDL{Action: astShowDatabases}
		}
	case 23:
		//line sql.y:233
		{
			yyVAL.statement = &DDL{Action: astShowTables, Name: yyS[yypt-0].str}
		}
	case 24:
		//line sql.y:237
		{
			yyVAL.statement = &DDL{Action: astShowIndex, Name: yyS[yypt-0].str}
		}
	case 25:
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: astShowColumns, Name: yyS[yypt-0].str}
		}
	case 26:
		//line sql.y:245
		{
			yyVAL.statement = &DDL{Action: astShowFullColumns, Name: yyS[yypt-0].str}
		}
	case 27:
		//line sql.y:251
		{
			yyVAL.statement = &CreateTable{IfNotExists: yyS[yypt-5].str, Name: yyS[yypt-4].str, Defs: yyS[yypt-2].tableDefs}
		}
	case 28:
		//line sql.y:255
		{
			yyVAL.statement = &CreateIndex{Name: yyS[yypt-4].str, TableName: yyS[yypt-1].str, Constraint: yyS[yypt-6].str}
		}
	case 29:
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: astCreateView, NewName: yyS[yypt-1].str}
		}
	case 30:
		//line sql.y:263
		{
			yyVAL.statement = &CreateDatabase{IfNotExists: yyS[yypt-1].str, Name: yyS[yypt-0].str}
		}
	case 31:
		//line sql.y:269
		{
			yyVAL.tableDefs = TableDefs{yyS[yypt-0].tableDef}
		}
	case 32:
		//line sql.y:273
		{
			yyVAL.tableDefs = append(yyVAL.tableDefs, yyS[yypt-0].tableDef)
		}
	case 33:
		//line sql.y:279
		{
			yyVAL.tableDef = &ColumnTableDef{Name: yyS[yypt-3].str, Type: yyS[yypt-2].str, Null: yyS[yypt-1].str, Constraint: yyS[yypt-0].str}
		}
	case 34:
		//line sql.y:283
		{
			yyVAL.tableDef = &IndexTableDef{Name: yyS[yypt-3].str, Constraint: yyS[yypt-5].str, Columns: yyS[yypt-1].str2}
		}
	case 35:
		//line sql.y:290
		{
			yyVAL.str = "BIT"
		}
	case 36:
		//line sql.y:294
		{
			yyVAL.str = "INT"
		}
	case 37:
		//line sql.y:298
		{
			yyVAL.str = "TINYINT"
		}
	case 38:
		//line sql.y:302
		{
			yyVAL.str = "SMALLINT"
		}
	case 39:
		//line sql.y:306
		{
			yyVAL.str = "MEDIUMINT"
		}
	case 40:
		//line sql.y:310
		{
			yyVAL.str = "BIGINT"
		}
	case 41:
		//line sql.y:314
		{
			yyVAL.str = "INTEGER"
		}
	case 42:
		//line sql.y:318
		{
			yyVAL.str = "REAL"
		}
	case 43:
		//line sql.y:322
		{
			yyVAL.str = "DOUBLE"
		}
	case 44:
		//line sql.y:326
		{
			yyVAL.str = "FLOAT"
		}
	case 45:
		//line sql.y:330
		{
			yyVAL.str = "DECIMAL"
		}
	case 46:
		//line sql.y:334
		{
			yyVAL.str = "NUMERIC"
		}
	case 47:
		//line sql.y:338
		{
			yyVAL.str = "DATE"
		}
	case 48:
		//line sql.y:342
		{
			yyVAL.str = "TIME"
		}
	case 49:
		//line sql.y:346
		{
			yyVAL.str = "DATETIME"
		}
	case 50:
		//line sql.y:350
		{
			yyVAL.str = "TIMESTAMP"
		}
	case 51:
		//line sql.y:354
		{
			yyVAL.str = "CHAR"
		}
	case 52:
		//line sql.y:358
		{
			yyVAL.str = "VARCHAR"
		}
	case 53:
		//line sql.y:362
		{
			yyVAL.str = "BINARY"
		}
	case 54:
		//line sql.y:366
		{
			yyVAL.str = "VARBINARY"
		}
	case 55:
		//line sql.y:370
		{
			yyVAL.str = "TEXT"
		}
	case 56:
		//line sql.y:374
		{
			yyVAL.str = "TINYTEXT"
		}
	case 57:
		//line sql.y:378
		{
			yyVAL.str = "MEDIUMTEXT"
		}
	case 58:
		//line sql.y:382
		{
			yyVAL.str = "LONGTEXT"
		}
	case 59:
		//line sql.y:386
		{
			yyVAL.str = "BLOB"
		}
	case 60:
		//line sql.y:390
		{
			yyVAL.str = "TINYBLOB"
		}
	case 61:
		//line sql.y:394
		{
			yyVAL.str = "MEDIUMBLOB"
		}
	case 62:
		//line sql.y:398
		{
			yyVAL.str = "LONGBLOB"
		}
	case 63:
		//line sql.y:402
		{
			yyVAL.str = "ENUM"
		}
	case 64:
		//line sql.y:406
		{
			yyVAL.str = "SET"
		}
	case 65:
		//line sql.y:411
		{
			yyVAL.str = ""
		}
	case 66:
		//line sql.y:413
		{
			yyVAL.str = astNull
		}
	case 67:
		//line sql.y:415
		{
			yyVAL.str = astNotNull
		}
	case 68:
		//line sql.y:418
		{
			yyVAL.str = ""
		}
	case 69:
		//line sql.y:420
		{
			yyVAL.str = astPrimaryKey
		}
	case 70:
		//line sql.y:422
		{
			yyVAL.str = astKey
		}
	case 71:
		//line sql.y:424
		{
			yyVAL.str = astUnique
		}
	case 72:
		//line sql.y:426
		{
			yyVAL.str = astUnique
		}
	case 73:
		//line sql.y:430
		{
			yyVAL.statement = &DDL{Action: astAlterTable, Name: yyS[yypt-2].str, NewName: yyS[yypt-2].str}
		}
	case 74:
		//line sql.y:434
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: astRenameTable, Name: yyS[yypt-3].str, NewName: yyS[yypt-0].str}
		}
	case 75:
		//line sql.y:439
		{
			yyVAL.statement = &DDL{Action: astAlterView, Name: yyS[yypt-1].str, NewName: yyS[yypt-1].str}
		}
	case 76:
		//line sql.y:445
		{
			yyVAL.statement = &DDL{Action: astRenameTable, Name: yyS[yypt-2].str, NewName: yyS[yypt-0].str}
		}
	case 77:
		//line sql.y:451
		{
			yyVAL.statement = &DDL{Action: astTruncateTable, Name: yyS[yypt-0].str}
		}
	case 78:
		//line sql.y:457
		{
			yyVAL.statement = &DDL{Action: astDropTable, Name: yyS[yypt-0].str}
		}
	case 79:
		//line sql.y:461
		{
			yyVAL.statement = &DDL{Action: astDropIndex, Name: yyS[yypt-2].str, NewName: yyS[yypt-0].str}
		}
	case 80:
		//line sql.y:465
		{
			yyVAL.statement = &DDL{Action: astDropView, Name: yyS[yypt-1].str}
		}
	case 81:
		//line sql.y:469
		{
			yyVAL.statement = &DDL{Action: astDropDatabase, Name: yyS[yypt-1].str}
		}
	case 82:
		//line sql.y:474
		{
			setAllowComments(yylex, true)
		}
	case 83:
		//line sql.y:478
		{
			yyVAL.str2 = yyS[yypt-0].str2
			setAllowComments(yylex, false)
		}
	case 84:
		//line sql.y:484
		{
			yyVAL.str2 = nil
		}
	case 85:
		//line sql.y:488
		{
			yyVAL.str2 = append(yyS[yypt-1].str2, yyS[yypt-0].str)
		}
	case 86:
		//line sql.y:494
		{
			yyVAL.str = astUnion
		}
	case 87:
		//line sql.y:498
		{
			yyVAL.str = astUnionAll
		}
	case 88:
		//line sql.y:502
		{
			yyVAL.str = astSetMinus
		}
	case 89:
		//line sql.y:506
		{
			yyVAL.str = astExcept
		}
	case 90:
		//line sql.y:510
		{
			yyVAL.str = astIntersect
		}
	case 91:
		//line sql.y:515
		{
			yyVAL.str = ""
		}
	case 92:
		//line sql.y:519
		{
			yyVAL.str = astDistinct
		}
	case 93:
		//line sql.y:525
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 94:
		//line sql.y:529
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 95:
		//line sql.y:535
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 96:
		//line sql.y:539
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].str}
		}
	case 97:
		//line sql.y:543
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].str}
		}
	case 98:
		//line sql.y:549
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 99:
		//line sql.y:553
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 100:
		//line sql.y:558
		{
			yyVAL.str = ""
		}
	case 101:
		//line sql.y:562
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 102:
		//line sql.y:566
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 103:
		//line sql.y:572
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 104:
		//line sql.y:576
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 105:
		//line sql.y:582
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].str, Hints: yyS[yypt-0].indexHints}
		}
	case 106:
		//line sql.y:586
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 107:
		//line sql.y:590
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 108:
		//line sql.y:594
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, Cond: &OnJoinCond{yyS[yypt-0].boolExpr}}
		}
	case 109:
		//line sql.y:598
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-6].tableExpr, Join: yyS[yypt-5].str, RightExpr: yyS[yypt-4].tableExpr, Cond: &UsingJoinCond{yyS[yypt-1].columns}}
		}
	case 110:
		//line sql.y:603
		{
			yyVAL.str = ""
		}
	case 111:
		//line sql.y:607
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 112:
		//line sql.y:611
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 113:
		//line sql.y:617
		{
			yyVAL.str = astJoin
		}
	case 114:
		//line sql.y:621
		{
			yyVAL.str = astStraightJoin
		}
	case 115:
		//line sql.y:625
		{
			yyVAL.str = astLeftJoin
		}
	case 116:
		//line sql.y:629
		{
			yyVAL.str = astLeftJoin
		}
	case 117:
		//line sql.y:633
		{
			yyVAL.str = astRightJoin
		}
	case 118:
		//line sql.y:637
		{
			yyVAL.str = astRightJoin
		}
	case 119:
		//line sql.y:641
		{
			yyVAL.str = astJoin
		}
	case 120:
		//line sql.y:645
		{
			yyVAL.str = astCrossJoin
		}
	case 121:
		//line sql.y:649
		{
			yyVAL.str = astNaturalJoin
		}
	case 122:
		//line sql.y:655
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].str}
		}
	case 123:
		//line sql.y:659
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 124:
		//line sql.y:663
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 125:
		//line sql.y:669
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].str}
		}
	case 126:
		//line sql.y:673
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 127:
		//line sql.y:678
		{
			yyVAL.indexHints = nil
		}
	case 128:
		//line sql.y:682
		{
			yyVAL.indexHints = &IndexHints{Type: astUse, Indexes: yyS[yypt-1].str2}
		}
	case 129:
		//line sql.y:686
		{
			yyVAL.indexHints = &IndexHints{Type: astIgnore, Indexes: yyS[yypt-1].str2}
		}
	case 130:
		//line sql.y:690
		{
			yyVAL.indexHints = &IndexHints{Type: astForce, Indexes: yyS[yypt-1].str2}
		}
	case 131:
		//line sql.y:696
		{
			yyVAL.str2 = []string{yyS[yypt-0].str}
		}
	case 132:
		//line sql.y:700
		{
			yyVAL.str2 = append(yyS[yypt-2].str2, yyS[yypt-0].str)
		}
	case 133:
		//line sql.y:705
		{
			yyVAL.boolExpr = nil
		}
	case 134:
		//line sql.y:709
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 135:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 136:
		//line sql.y:716
		{
			yyVAL.boolExpr = &AndExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 137:
		//line sql.y:720
		{
			yyVAL.boolExpr = &OrExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 138:
		//line sql.y:724
		{
			yyVAL.boolExpr = &NotExpr{Op: string(yyS[yypt-1].str), Expr: yyS[yypt-0].boolExpr}
		}
	case 139:
		//line sql.y:728
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 140:
		//line sql.y:734
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 141:
		//line sql.y:738
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astIn, Right: yyS[yypt-0].tuple}
		}
	case 142:
		//line sql.y:742
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotIn, Right: yyS[yypt-0].tuple}
		}
	case 143:
		//line sql.y:746
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astLike, Right: yyS[yypt-0].valExpr}
		}
	case 144:
		//line sql.y:750
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotLike, Right: yyS[yypt-0].valExpr}
		}
	case 145:
		//line sql.y:754
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: astBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 146:
		//line sql.y:758
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: astNotBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 147:
		//line sql.y:762
		{
			yyVAL.boolExpr = &NullCheck{Operator: astNull, Expr: yyS[yypt-2].valExpr}
		}
	case 148:
		//line sql.y:766
		{
			yyVAL.boolExpr = &NullCheck{Operator: astNotNull, Expr: yyS[yypt-3].valExpr}
		}
	case 149:
		//line sql.y:770
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 150:
		//line sql.y:776
		{
			yyVAL.str = astEQ
		}
	case 151:
		//line sql.y:780
		{
			yyVAL.str = astLT
		}
	case 152:
		//line sql.y:784
		{
			yyVAL.str = astGT
		}
	case 153:
		//line sql.y:788
		{
			yyVAL.str = astLE
		}
	case 154:
		//line sql.y:792
		{
			yyVAL.str = astGE
		}
	case 155:
		//line sql.y:796
		{
			yyVAL.str = astNE
		}
	case 156:
		//line sql.y:800
		{
			yyVAL.str = astNSE
		}
	case 157:
		//line sql.y:806
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 158:
		//line sql.y:810
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 159:
		//line sql.y:816
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 160:
		//line sql.y:820
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 161:
		//line sql.y:826
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 162:
		//line sql.y:830
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 163:
		//line sql.y:836
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 164:
		//line sql.y:842
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 165:
		//line sql.y:846
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 166:
		//line sql.y:852
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 167:
		//line sql.y:856
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 168:
		//line sql.y:860
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 169:
		//line sql.y:864
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitand, Right: yyS[yypt-0].valExpr}
		}
	case 170:
		//line sql.y:868
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitor, Right: yyS[yypt-0].valExpr}
		}
	case 171:
		//line sql.y:872
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitxor, Right: yyS[yypt-0].valExpr}
		}
	case 172:
		//line sql.y:876
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astPlus, Right: yyS[yypt-0].valExpr}
		}
	case 173:
		//line sql.y:880
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMinus, Right: yyS[yypt-0].valExpr}
		}
	case 174:
		//line sql.y:884
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMult, Right: yyS[yypt-0].valExpr}
		}
	case 175:
		//line sql.y:888
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astDiv, Right: yyS[yypt-0].valExpr}
		}
	case 176:
		//line sql.y:892
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMod, Right: yyS[yypt-0].valExpr}
		}
	case 177:
		//line sql.y:896
		{
			if num, ok := yyS[yypt-0].valExpr.(NumVal); ok {
				switch yyS[yypt-1].byt {
				case '-':
					yyVAL.valExpr = NumVal("-" + string(num))
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
			}
		}
	case 178:
		//line sql.y:911
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-2].str)}
		}
	case 179:
		//line sql.y:915
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 180:
		//line sql.y:919
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-4].str), Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 181:
		//line sql.y:923
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 182:
		//line sql.y:927
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 183:
		//line sql.y:933
		{
			yyVAL.str = "IF"
		}
	case 184:
		//line sql.y:937
		{
			yyVAL.str = "VALUES"
		}
	case 185:
		//line sql.y:943
		{
			yyVAL.byt = astUnaryPlus
		}
	case 186:
		//line sql.y:947
		{
			yyVAL.byt = astUnaryMinus
		}
	case 187:
		//line sql.y:951
		{
			yyVAL.byt = astTilda
		}
	case 188:
		//line sql.y:957
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 189:
		//line sql.y:962
		{
			yyVAL.valExpr = nil
		}
	case 190:
		//line sql.y:966
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 191:
		//line sql.y:972
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 192:
		//line sql.y:976
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 193:
		//line sql.y:982
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 194:
		//line sql.y:987
		{
			yyVAL.valExpr = nil
		}
	case 195:
		//line sql.y:991
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 196:
		//line sql.y:997
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].str}
		}
	case 197:
		//line sql.y:1001
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 198:
		//line sql.y:1007
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].str)
		}
	case 199:
		//line sql.y:1011
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].str)
		}
	case 200:
		//line sql.y:1015
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].str)
		}
	case 201:
		//line sql.y:1019
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 202:
		//line sql.y:1024
		{
			yyVAL.valExprs = nil
		}
	case 203:
		//line sql.y:1028
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 204:
		//line sql.y:1033
		{
			yyVAL.boolExpr = nil
		}
	case 205:
		//line sql.y:1037
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 206:
		//line sql.y:1042
		{
			yyVAL.orderBy = nil
		}
	case 207:
		//line sql.y:1046
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 208:
		//line sql.y:1052
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 209:
		//line sql.y:1056
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 210:
		//line sql.y:1062
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 211:
		//line sql.y:1067
		{
			yyVAL.str = astAsc
		}
	case 212:
		//line sql.y:1071
		{
			yyVAL.str = astAsc
		}
	case 213:
		//line sql.y:1075
		{
			yyVAL.str = astDesc
		}
	case 214:
		//line sql.y:1080
		{
			yyVAL.limit = nil
		}
	case 215:
		//line sql.y:1084
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 216:
		//line sql.y:1088
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 217:
		//line sql.y:1092
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-0].valExpr, Rowcount: yyS[yypt-2].valExpr}
		}
	case 218:
		//line sql.y:1097
		{
			yyVAL.str = ""
		}
	case 219:
		//line sql.y:1101
		{
			yyVAL.str = astForUpdate
		}
	case 220:
		//line sql.y:1105
		{
			if yyS[yypt-1].str != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyS[yypt-0].str != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = astShareMode
		}
	case 221:
		//line sql.y:1118
		{
			yyVAL.columns = nil
		}
	case 222:
		//line sql.y:1122
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 223:
		//line sql.y:1128
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 224:
		//line sql.y:1132
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 225:
		//line sql.y:1137
		{
			yyVAL.updateExprs = nil
		}
	case 226:
		//line sql.y:1141
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 227:
		//line sql.y:1147
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 228:
		//line sql.y:1151
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 229:
		//line sql.y:1157
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 230:
		//line sql.y:1162
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		//line sql.y:1164
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		//line sql.y:1167
		{
			yyVAL.str = ""
		}
	case 233:
		//line sql.y:1169
		{
			yyVAL.str = astIfNotExists
		}
	case 234:
		//line sql.y:1172
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		//line sql.y:1174
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		//line sql.y:1178
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		//line sql.y:1180
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		//line sql.y:1182
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		//line sql.y:1184
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		//line sql.y:1186
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		//line sql.y:1189
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		//line sql.y:1191
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		//line sql.y:1194
		{
			yyVAL.str = ""
		}
	case 244:
		//line sql.y:1196
		{
			yyVAL.str = astUnique
		}
	case 245:
		//line sql.y:1199
		{
			yyVAL.str = ""
		}
	case 246:
		//line sql.y:1201
		{
			yyVAL.str = yyS[yypt-1].str
		}
	case 247:
		//line sql.y:1204
		{
			yyVAL.str = ""
		}
	case 248:
		//line sql.y:1206
		{
			yyVAL.str = yyS[yypt-3].str
		}
	case 249:
		//line sql.y:1209
		{
			yyVAL.str = ""
		}
	case 250:
		//line sql.y:1211
		{
			yyVAL.str = yyS[yypt-2].str
		}
	case 251:
		//line sql.y:1214
		{
			yyVAL.str = ""
		}
	case 252:
		//line sql.y:1216
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 253:
		//line sql.y:1219
		{
			yyVAL.str = ""
		}
	case 254:
		//line sql.y:1221
		{
			yyVAL.str = astUnsigned
		}
	case 255:
		//line sql.y:1224
		{
			yyVAL.empty = struct{}{}
		}
	case 256:
		//line sql.y:1226
		{
			yyVAL.empty = struct{}{}
		}
	case 257:
		//line sql.y:1229
		{
			yyVAL.str = ""
		}
	case 258:
		//line sql.y:1231
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 259:
		//line sql.y:1235
		{
			yyVAL.str = strings.ToLower(yyS[yypt-0].str)
		}
	case 260:
		//line sql.y:1238
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
