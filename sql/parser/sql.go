//line sql.y:6
package parser

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func forceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
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
const tokID = 57378
const tokString = 57379
const tokNumber = 57380
const tokValueArg = 57381
const tokComment = 57382
const tokLE = 57383
const tokGE = 57384
const tokNE = 57385
const tokNullSafeEqual = 57386
const tokUnion = 57387
const tokMinus = 57388
const tokExcept = 57389
const tokIntersect = 57390
const tokJoin = 57391
const tokStraightJoin = 57392
const tokLeft = 57393
const tokRight = 57394
const tokInner = 57395
const tokOuter = 57396
const tokCross = 57397
const tokNatural = 57398
const tokUse = 57399
const tokForce = 57400
const tokOn = 57401
const tokUsing = 57402
const tokAnd = 57403
const tokOr = 57404
const tokNot = 57405
const tokUnary = 57406
const tokCase = 57407
const tokWhen = 57408
const tokThen = 57409
const tokElse = 57410
const tokEnd = 57411
const tokCreate = 57412
const tokAlter = 57413
const tokDrop = 57414
const tokRename = 57415
const tokTruncate = 57416
const tokShow = 57417
const tokDatabase = 57418
const tokTable = 57419
const tokTables = 57420
const tokIndex = 57421
const tokView = 57422
const tokColumns = 57423
const tokFull = 57424
const tokTo = 57425
const tokIgnore = 57426
const tokIf = 57427
const tokUnique = 57428

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

const yyNprod = 206
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 581

var yyAct = []int{

	104, 304, 173, 376, 66, 95, 339, 101, 175, 257,
	214, 260, 296, 67, 251, 102, 176, 3, 191, 28,
	29, 30, 31, 86, 90, 91, 149, 150, 112, 271,
	272, 273, 274, 275, 386, 276, 277, 386, 69, 71,
	386, 77, 267, 302, 57, 68, 79, 46, 43, 137,
	82, 45, 49, 144, 75, 87, 50, 47, 351, 321,
	323, 241, 96, 302, 144, 144, 243, 350, 330, 133,
	349, 244, 78, 81, 127, 128, 325, 130, 252, 132,
	52, 134, 388, 51, 282, 387, 138, 131, 385, 141,
	142, 384, 107, 200, 146, 322, 252, 111, 294, 148,
	117, 329, 177, 126, 172, 174, 178, 94, 108, 109,
	110, 301, 291, 289, 242, 122, 99, 297, 149, 150,
	115, 185, 69, 149, 150, 69, 189, 195, 194, 68,
	196, 124, 68, 332, 345, 346, 182, 231, 193, 72,
	98, 210, 297, 263, 113, 114, 92, 96, 220, 195,
	140, 118, 136, 348, 224, 222, 223, 229, 230, 347,
	233, 234, 235, 236, 237, 238, 239, 240, 225, 85,
	219, 116, 218, 39, 40, 63, 41, 42, 221, 207,
	232, 319, 245, 96, 96, 162, 163, 164, 69, 69,
	56, 53, 256, 54, 55, 68, 258, 318, 262, 205,
	197, 264, 208, 255, 201, 247, 249, 317, 259, 125,
	315, 211, 212, 192, 327, 316, 15, 157, 158, 159,
	160, 161, 162, 163, 164, 88, 89, 281, 313, 124,
	284, 285, 268, 314, 243, 358, 334, 111, 120, 192,
	117, 123, 283, 143, 187, 218, 288, 70, 108, 109,
	110, 96, 204, 206, 203, 188, 179, 269, 295, 76,
	115, 369, 368, 367, 307, 293, 366, 179, 183, 300,
	299, 303, 265, 290, 360, 361, 28, 29, 30, 31,
	311, 312, 181, 124, 113, 114, 180, 72, 144, 328,
	119, 118, 160, 161, 162, 163, 164, 331, 218, 218,
	280, 70, 15, 69, 217, 336, 80, 326, 337, 340,
	335, 116, 324, 216, 308, 209, 279, 157, 158, 159,
	160, 161, 162, 163, 164, 271, 272, 273, 274, 275,
	352, 276, 277, 217, 190, 353, 147, 15, 16, 17,
	18, 135, 216, 64, 382, 84, 83, 245, 121, 363,
	355, 365, 72, 364, 362, 354, 333, 287, 15, 371,
	372, 340, 383, 62, 374, 373, 19, 69, 377, 377,
	377, 69, 378, 379, 258, 380, 375, 341, 68, 248,
	390, 107, 254, 198, 139, 60, 111, 391, 58, 117,
	305, 392, 261, 393, 344, 20, 94, 108, 109, 110,
	226, 306, 227, 228, 343, 99, 32, 310, 192, 115,
	129, 74, 73, 65, 389, 15, 370, 22, 23, 26,
	24, 25, 21, 34, 35, 36, 37, 38, 15, 98,
	33, 107, 199, 113, 114, 92, 111, 44, 266, 117,
	118, 202, 48, 186, 381, 359, 70, 108, 109, 110,
	338, 342, 309, 107, 292, 99, 184, 250, 111, 115,
	116, 117, 246, 106, 103, 105, 298, 100, 70, 108,
	109, 110, 253, 151, 97, 111, 320, 99, 117, 98,
	215, 115, 270, 113, 114, 70, 108, 109, 110, 213,
	118, 93, 278, 145, 179, 59, 27, 61, 115, 357,
	14, 98, 13, 12, 11, 113, 114, 10, 9, 8,
	116, 7, 118, 6, 152, 156, 154, 155, 5, 4,
	2, 1, 113, 114, 0, 0, 0, 0, 0, 118,
	0, 0, 116, 168, 169, 170, 171, 356, 165, 166,
	167, 157, 158, 159, 160, 161, 162, 163, 164, 116,
	0, 0, 0, 157, 158, 159, 160, 161, 162, 163,
	164, 153, 157, 158, 159, 160, 161, 162, 163, 164,
	286, 0, 0, 157, 158, 159, 160, 161, 162, 163,
	164,
}
var yyPact = []int{

	332, -1000, -1000, 226, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 80, -44, -43, -9, -12, 99, 423, 370, -1000,
	-1000, -1000, 366, -1000, 333, 307, 404, 265, 251, -1000,
	403, 402, -42, -59, -22, 251, -59, -1000, -19, 251,
	-1000, 310, 309, -77, 251, -77, -77, -1000, -1000, 71,
	-1000, 250, 307, 314, 37, 307, 175, -1000, 163, -1000,
	25, -1000, -1000, 251, 251, 401, 251, 18, 251, -1000,
	251, 305, -1000, -49, -1000, 251, 363, 85, 251, 251,
	234, -1000, -1000, 316, 21, 56, 492, -1000, 432, 410,
	-1000, -1000, -1000, 449, 241, 237, -1000, 223, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 449, -1000,
	210, 265, 298, 398, 265, 449, 251, -1000, -1000, 251,
	-1000, 362, 27, -1000, -1000, 166, -1000, 279, -1000, -1000,
	251, -1000, -1000, 268, 71, -1000, -1000, 251, 103, 432,
	432, 449, 222, 378, 449, 449, 111, 449, 449, 449,
	449, 449, 449, 449, 449, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 492, -41, 12, -31, 492, -1000, 211,
	360, 71, -1000, 423, -3, 471, 353, 265, 265, 229,
	-1000, 379, 432, -1000, 471, -1000, -1000, -1000, -1000, 78,
	251, -1000, -1000, -56, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 203, 270, 280, 297, 6, -1000, -1000,
	-1000, -1000, -1000, -1000, 471, -1000, 222, 449, 449, 471,
	503, -1000, 331, 219, 219, 219, 110, 110, -1000, -1000,
	-1000, -1000, -1000, 449, -1000, 471, -1000, 11, 71, 10,
	15, -1000, 432, 52, 222, 226, 77, 9, -1000, 379,
	375, 387, 56, 251, -1000, -1000, 278, -1000, 396, 268,
	268, -1000, -1000, 173, 155, 152, 142, 126, -4, -1000,
	276, -26, 271, -1000, 471, 147, 449, -1000, 471, -1000,
	-1, -1000, -16, -1000, 449, 51, -1000, 325, 182, -1000,
	-1000, -1000, 265, 375, -1000, 449, 449, -1000, -1000, 392,
	380, 270, 69, -1000, 104, -1000, 98, -1000, -1000, -1000,
	-1000, -24, -27, -36, -1000, -1000, -1000, 449, 471, -1000,
	-1000, 471, 449, 323, 222, -1000, -1000, 483, 181, -1000,
	247, -1000, 379, 432, 449, 432, 221, -1000, -1000, 218,
	217, 216, 471, 471, 409, -1000, 449, 449, 449, -1000,
	-1000, -1000, 375, 56, 180, 56, 265, 251, 251, 251,
	265, 471, 471, -1000, 327, -11, -14, -1000, -17, -20,
	175, -1000, 407, 358, -1000, -1000, 251, -1000, -1000, -1000,
	251, -1000, 251, -1000,
}
var yyPgo = []int{

	0, 521, 520, 16, 519, 518, 513, 511, 509, 508,
	507, 504, 503, 502, 500, 406, 497, 496, 495, 24,
	25, 493, 492, 491, 489, 10, 482, 480, 175, 476,
	3, 18, 5, 474, 473, 472, 467, 2, 15, 8,
	466, 465, 28, 464, 7, 463, 457, 14, 456, 454,
	452, 451, 11, 450, 6, 445, 1, 444, 443, 9,
	12, 4, 13, 169, 259, 442, 441, 438, 437, 432,
	0, 69, 430,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 3, 3, 4, 4, 5, 6,
	7, 8, 9, 9, 9, 9, 10, 10, 10, 10,
	11, 11, 11, 12, 13, 14, 14, 14, 14, 72,
	15, 16, 16, 17, 17, 17, 17, 17, 18, 18,
	19, 19, 20, 20, 20, 23, 23, 21, 21, 21,
	24, 24, 25, 25, 25, 25, 25, 22, 22, 22,
	26, 26, 26, 26, 26, 26, 26, 26, 26, 27,
	27, 27, 28, 28, 29, 29, 29, 29, 30, 30,
	31, 31, 32, 32, 32, 32, 32, 33, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 34, 34, 34,
	34, 34, 34, 34, 35, 35, 40, 40, 38, 38,
	42, 39, 39, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	41, 41, 43, 43, 43, 45, 48, 48, 46, 46,
	47, 49, 49, 44, 44, 36, 36, 36, 36, 50,
	50, 51, 51, 52, 52, 53, 53, 54, 55, 55,
	55, 56, 56, 56, 56, 57, 57, 57, 58, 58,
	59, 59, 60, 60, 61, 61, 62, 63, 63, 64,
	64, 65, 65, 66, 66, 66, 66, 66, 67, 67,
	68, 68, 69, 69, 70, 71,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 12, 3, 7, 7, 8, 7,
	3, 3, 2, 4, 4, 5, 5, 8, 4, 5,
	6, 7, 4, 5, 3, 4, 5, 5, 5, 0,
	2, 0, 2, 1, 2, 1, 1, 1, 0, 1,
	1, 3, 1, 2, 3, 1, 1, 0, 1, 2,
	1, 3, 3, 3, 3, 5, 7, 0, 1, 2,
	1, 1, 2, 3, 2, 3, 2, 2, 2, 1,
	3, 1, 1, 3, 0, 5, 5, 5, 1, 3,
	0, 2, 1, 3, 3, 2, 3, 3, 3, 4,
	3, 4, 5, 6, 3, 4, 2, 1, 1, 1,
	1, 1, 1, 1, 2, 1, 1, 3, 3, 1,
	3, 1, 3, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 2, 3, 4, 5, 4, 1,
	1, 1, 1, 1, 1, 5, 0, 1, 1, 2,
	4, 0, 2, 1, 3, 1, 1, 1, 1, 0,
	3, 0, 2, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 2, 4, 4, 0, 2, 4, 0, 3,
	1, 3, 0, 5, 1, 3, 3, 0, 2, 0,
	3, 0, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, -14, 5, 6, 7, 8, 34,
	63, 90, 85, 86, 88, 89, 87, -17, 50, 51,
	52, 53, -15, -72, -15, -15, -15, -15, -15, 93,
	94, 96, 97, 92, -68, 95, 91, 101, -65, 95,
	99, 92, 92, 92, 94, 95, 91, -3, 18, -18,
	19, -16, 30, -28, 36, 9, -61, -62, -44, -70,
	36, -70, 36, 9, 9, 96, -64, 100, 94, -70,
	-64, 92, -70, 36, 36, -63, 100, -70, -63, -63,
	-19, -20, 75, -23, 36, -32, -37, -33, 69, 45,
	-36, -44, -38, -43, -70, -41, -45, 21, 37, 38,
	39, 26, -42, 73, 74, 49, 100, 29, 80, 40,
	-28, 34, 78, -28, 54, 46, 78, -70, -70, 9,
	-70, 69, -70, -71, -70, 36, -71, 98, -70, 21,
	65, -70, -70, 9, 54, -21, -70, 20, 78, 67,
	68, -34, 22, 69, 24, 25, 23, 70, 71, 72,
	73, 74, 75, 76, 77, 46, 47, 48, 41, 42,
	43, 44, -32, -37, -32, -39, -3, -37, -37, 45,
	45, 45, -42, 45, -48, -37, -58, 34, 45, -61,
	36, -31, 10, -62, -37, -70, -70, -71, 21, -69,
	66, -71, -66, 88, 86, 33, 87, 13, 36, 36,
	-70, -71, -71, -24, -25, -27, 45, 36, -42, -20,
	-70, 75, -32, -32, -37, -38, 22, 24, 25, -37,
	-37, 26, 69, -37, -37, -37, -37, -37, -37, -37,
	-37, 102, 102, 54, 102, -37, 102, -19, 19, -19,
	-46, -47, 81, -35, 29, -3, -61, -59, -44, -31,
	-52, 13, -32, 65, -70, -71, -67, 98, -31, 54,
	-26, 55, 56, 57, 58, 59, 61, 62, -22, 36,
	20, -25, 78, -38, -37, -37, 67, 26, -37, 102,
	-19, 102, -49, -47, 83, -32, -60, 65, -40, -38,
	-60, 102, 54, -52, -56, 15, 14, -70, 36, -50,
	11, -25, -25, 55, 60, 55, 60, 55, 55, 55,
	-29, 63, 99, 64, 36, 102, 36, 67, -37, 102,
	84, -37, 82, 31, 54, -44, -56, -37, -53, -54,
	-37, -71, -51, 12, 14, 65, 66, 55, 55, 94,
	94, 94, -37, -37, 32, -38, 54, 16, 54, -55,
	27, 28, -52, -32, -39, -32, 45, 45, 45, 45,
	7, -37, -37, -54, -56, -59, -30, -70, -30, -30,
	-61, -57, 17, 35, 102, 102, 54, 102, 102, 7,
	22, -70, -70, -70,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 39, 39, 39, 39, 39,
	39, 0, 200, 191, 0, 0, 0, 0, 43, 45,
	46, 47, 48, 41, 0, 0, 0, 0, 0, 22,
	0, 0, 0, 189, 0, 0, 189, 201, 0, 0,
	192, 0, 0, 187, 0, 187, 187, 15, 44, 0,
	49, 40, 0, 0, 82, 0, 20, 184, 0, 153,
	204, 21, 204, 0, 0, 0, 0, 0, 0, 205,
	0, 0, 205, 0, 34, 0, 0, 0, 0, 0,
	0, 50, 52, 57, 204, 55, 56, 92, 0, 0,
	123, 124, 125, 0, 153, 0, 139, 0, 155, 156,
	157, 158, 119, 142, 143, 144, 140, 141, 146, 42,
	178, 0, 0, 90, 0, 0, 0, 23, 24, 0,
	205, 0, 202, 28, 205, 0, 32, 0, 35, 188,
	0, 205, 205, 0, 0, 53, 58, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 107, 108, 109, 110, 111,
	112, 113, 95, 0, 0, 0, 0, 121, 134, 0,
	0, 0, 106, 0, 0, 147, 0, 0, 0, 90,
	83, 163, 0, 185, 186, 154, 25, 26, 190, 0,
	0, 29, 205, 198, 193, 194, 195, 196, 197, 33,
	36, 37, 38, 90, 60, 67, 0, 79, 81, 51,
	59, 54, 93, 94, 97, 98, 0, 0, 0, 100,
	0, 104, 0, 126, 127, 128, 129, 130, 131, 132,
	133, 96, 118, 0, 120, 121, 135, 0, 0, 0,
	151, 148, 0, 182, 0, 115, 182, 0, 180, 163,
	171, 0, 91, 0, 203, 30, 0, 199, 159, 0,
	0, 70, 71, 0, 0, 0, 0, 0, 84, 68,
	0, 0, 0, 99, 101, 0, 0, 105, 122, 136,
	0, 138, 0, 149, 0, 0, 16, 0, 114, 116,
	17, 179, 0, 171, 19, 0, 0, 205, 31, 161,
	0, 61, 64, 72, 0, 74, 0, 76, 77, 78,
	62, 0, 0, 0, 69, 63, 80, 0, 102, 137,
	145, 152, 0, 0, 0, 181, 18, 172, 164, 165,
	168, 27, 163, 0, 0, 0, 0, 73, 75, 0,
	0, 0, 103, 150, 0, 117, 0, 0, 0, 167,
	169, 170, 171, 162, 160, 65, 0, 0, 0, 0,
	0, 173, 174, 166, 175, 0, 0, 88, 0, 0,
	183, 14, 0, 0, 66, 85, 0, 86, 87, 176,
	0, 89, 0, 177,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 77, 70, 3,
	45, 102, 75, 73, 54, 74, 78, 76, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	47, 46, 48, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 72, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 71, 3, 49,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 50, 51, 52, 53, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101,
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
		//line sql.y:138
		{
			setParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:144
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
		//line sql.y:161
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].str2), Distinct: yyS[yypt-9].str, Exprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(astWhere, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(astHaving, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 15:
		//line sql.y:165
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 16:
		//line sql.y:171
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 17:
		//line sql.y:175
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
		//line sql.y:187
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].str2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:193
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 20:
		//line sql.y:199
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].str2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 21:
		//line sql.y:205
		{
			yyVAL.statement = &Use{Comments: Comments(yyS[yypt-1].str2), Name: yyS[yypt-0].str}
		}
	case 22:
		//line sql.y:211
		{
			yyVAL.statement = &DDL{Action: astShowTables}
		}
	case 23:
		//line sql.y:215
		{
			yyVAL.statement = &DDL{Action: astShowIndex, Name: yyS[yypt-0].str}
		}
	case 24:
		//line sql.y:219
		{
			yyVAL.statement = &DDL{Action: astShowColumns, Name: yyS[yypt-0].str}
		}
	case 25:
		//line sql.y:223
		{
			yyVAL.statement = &DDL{Action: astShowFullColumns, Name: yyS[yypt-0].str}
		}
	case 26:
		//line sql.y:229
		{
			yyVAL.statement = &DDL{Action: astCreateTable, NewName: yyS[yypt-1].str}
		}
	case 27:
		//line sql.y:233
		{
			yyVAL.statement = &DDL{Action: astCreateIndex, Name: yyS[yypt-4].str, NewName: yyS[yypt-1].str}
		}
	case 28:
		//line sql.y:237
		{
			yyVAL.statement = &DDL{Action: astCreateView, NewName: yyS[yypt-1].str}
		}
	case 29:
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: astCreateDatabase, NewName: yyS[yypt-1].str}
		}
	case 30:
		//line sql.y:247
		{
			yyVAL.statement = &DDL{Action: astAlterTable, Name: yyS[yypt-2].str, NewName: yyS[yypt-2].str}
		}
	case 31:
		//line sql.y:251
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: astRenameTable, Name: yyS[yypt-3].str, NewName: yyS[yypt-0].str}
		}
	case 32:
		//line sql.y:256
		{
			yyVAL.statement = &DDL{Action: astAlterView, Name: yyS[yypt-1].str, NewName: yyS[yypt-1].str}
		}
	case 33:
		//line sql.y:262
		{
			yyVAL.statement = &DDL{Action: astRenameTable, Name: yyS[yypt-2].str, NewName: yyS[yypt-0].str}
		}
	case 34:
		//line sql.y:268
		{
			yyVAL.statement = &DDL{Action: astTruncateTable, Name: yyS[yypt-0].str}
		}
	case 35:
		//line sql.y:274
		{
			yyVAL.statement = &DDL{Action: astDropTable, Name: yyS[yypt-0].str}
		}
	case 36:
		//line sql.y:278
		{
			yyVAL.statement = &DDL{Action: astDropIndex, Name: yyS[yypt-2].str, NewName: yyS[yypt-0].str}
		}
	case 37:
		//line sql.y:282
		{
			yyVAL.statement = &DDL{Action: astDropView, Name: yyS[yypt-1].str}
		}
	case 38:
		//line sql.y:286
		{
			yyVAL.statement = &DDL{Action: astDropDatabase, Name: yyS[yypt-1].str}
		}
	case 39:
		//line sql.y:291
		{
			setAllowComments(yylex, true)
		}
	case 40:
		//line sql.y:295
		{
			yyVAL.str2 = yyS[yypt-0].str2
			setAllowComments(yylex, false)
		}
	case 41:
		//line sql.y:301
		{
			yyVAL.str2 = nil
		}
	case 42:
		//line sql.y:305
		{
			yyVAL.str2 = append(yyS[yypt-1].str2, yyS[yypt-0].str)
		}
	case 43:
		//line sql.y:311
		{
			yyVAL.str = astUnion
		}
	case 44:
		//line sql.y:315
		{
			yyVAL.str = astUnionAll
		}
	case 45:
		//line sql.y:319
		{
			yyVAL.str = astSetMinus
		}
	case 46:
		//line sql.y:323
		{
			yyVAL.str = astExcept
		}
	case 47:
		//line sql.y:327
		{
			yyVAL.str = astIntersect
		}
	case 48:
		//line sql.y:332
		{
			yyVAL.str = ""
		}
	case 49:
		//line sql.y:336
		{
			yyVAL.str = astDistinct
		}
	case 50:
		//line sql.y:342
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 51:
		//line sql.y:346
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 52:
		//line sql.y:352
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 53:
		//line sql.y:356
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].str}
		}
	case 54:
		//line sql.y:360
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].str}
		}
	case 55:
		//line sql.y:366
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 56:
		//line sql.y:370
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 57:
		//line sql.y:375
		{
			yyVAL.str = ""
		}
	case 58:
		//line sql.y:379
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 59:
		//line sql.y:383
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 60:
		//line sql.y:389
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 61:
		//line sql.y:393
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 62:
		//line sql.y:399
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].str, Hints: yyS[yypt-0].indexHints}
		}
	case 63:
		//line sql.y:403
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 64:
		//line sql.y:407
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 65:
		//line sql.y:411
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, Cond: &OnJoinCond{yyS[yypt-0].boolExpr}}
		}
	case 66:
		//line sql.y:415
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-6].tableExpr, Join: yyS[yypt-5].str, RightExpr: yyS[yypt-4].tableExpr, Cond: &UsingJoinCond{yyS[yypt-1].columns}}
		}
	case 67:
		//line sql.y:420
		{
			yyVAL.str = ""
		}
	case 68:
		//line sql.y:424
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 69:
		//line sql.y:428
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 70:
		//line sql.y:434
		{
			yyVAL.str = astJoin
		}
	case 71:
		//line sql.y:438
		{
			yyVAL.str = astStraightJoin
		}
	case 72:
		//line sql.y:442
		{
			yyVAL.str = astLeftJoin
		}
	case 73:
		//line sql.y:446
		{
			yyVAL.str = astLeftJoin
		}
	case 74:
		//line sql.y:450
		{
			yyVAL.str = astRightJoin
		}
	case 75:
		//line sql.y:454
		{
			yyVAL.str = astRightJoin
		}
	case 76:
		//line sql.y:458
		{
			yyVAL.str = astJoin
		}
	case 77:
		//line sql.y:462
		{
			yyVAL.str = astCrossJoin
		}
	case 78:
		//line sql.y:466
		{
			yyVAL.str = astNaturalJoin
		}
	case 79:
		//line sql.y:472
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].str}
		}
	case 80:
		//line sql.y:476
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 81:
		//line sql.y:480
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 82:
		//line sql.y:486
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].str}
		}
	case 83:
		//line sql.y:490
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 84:
		//line sql.y:495
		{
			yyVAL.indexHints = nil
		}
	case 85:
		//line sql.y:499
		{
			yyVAL.indexHints = &IndexHints{Type: astUse, Indexes: yyS[yypt-1].str2}
		}
	case 86:
		//line sql.y:503
		{
			yyVAL.indexHints = &IndexHints{Type: astIgnore, Indexes: yyS[yypt-1].str2}
		}
	case 87:
		//line sql.y:507
		{
			yyVAL.indexHints = &IndexHints{Type: astForce, Indexes: yyS[yypt-1].str2}
		}
	case 88:
		//line sql.y:513
		{
			yyVAL.str2 = []string{yyS[yypt-0].str}
		}
	case 89:
		//line sql.y:517
		{
			yyVAL.str2 = append(yyS[yypt-2].str2, yyS[yypt-0].str)
		}
	case 90:
		//line sql.y:522
		{
			yyVAL.boolExpr = nil
		}
	case 91:
		//line sql.y:526
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 92:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 93:
		//line sql.y:533
		{
			yyVAL.boolExpr = &AndExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 94:
		//line sql.y:537
		{
			yyVAL.boolExpr = &OrExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 95:
		//line sql.y:541
		{
			yyVAL.boolExpr = &NotExpr{Op: string(yyS[yypt-1].str), Expr: yyS[yypt-0].boolExpr}
		}
	case 96:
		//line sql.y:545
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 97:
		//line sql.y:551
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:555
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astIn, Right: yyS[yypt-0].tuple}
		}
	case 99:
		//line sql.y:559
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotIn, Right: yyS[yypt-0].tuple}
		}
	case 100:
		//line sql.y:563
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astLike, Right: yyS[yypt-0].valExpr}
		}
	case 101:
		//line sql.y:567
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotLike, Right: yyS[yypt-0].valExpr}
		}
	case 102:
		//line sql.y:571
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: astBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 103:
		//line sql.y:575
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: astNotBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 104:
		//line sql.y:579
		{
			yyVAL.boolExpr = &NullCheck{Operator: astIsNull, Expr: yyS[yypt-2].valExpr}
		}
	case 105:
		//line sql.y:583
		{
			yyVAL.boolExpr = &NullCheck{Operator: astIsNotNull, Expr: yyS[yypt-3].valExpr}
		}
	case 106:
		//line sql.y:587
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 107:
		//line sql.y:593
		{
			yyVAL.str = astEQ
		}
	case 108:
		//line sql.y:597
		{
			yyVAL.str = astLT
		}
	case 109:
		//line sql.y:601
		{
			yyVAL.str = astGT
		}
	case 110:
		//line sql.y:605
		{
			yyVAL.str = astLE
		}
	case 111:
		//line sql.y:609
		{
			yyVAL.str = astGE
		}
	case 112:
		//line sql.y:613
		{
			yyVAL.str = astNE
		}
	case 113:
		//line sql.y:617
		{
			yyVAL.str = astNSE
		}
	case 114:
		//line sql.y:623
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 115:
		//line sql.y:627
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 116:
		//line sql.y:633
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 117:
		//line sql.y:637
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 118:
		//line sql.y:643
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 119:
		//line sql.y:647
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 120:
		//line sql.y:653
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 121:
		//line sql.y:659
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:663
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 123:
		//line sql.y:669
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 124:
		//line sql.y:673
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 125:
		//line sql.y:677
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 126:
		//line sql.y:681
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitand, Right: yyS[yypt-0].valExpr}
		}
	case 127:
		//line sql.y:685
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitor, Right: yyS[yypt-0].valExpr}
		}
	case 128:
		//line sql.y:689
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitxor, Right: yyS[yypt-0].valExpr}
		}
	case 129:
		//line sql.y:693
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astPlus, Right: yyS[yypt-0].valExpr}
		}
	case 130:
		//line sql.y:697
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMinus, Right: yyS[yypt-0].valExpr}
		}
	case 131:
		//line sql.y:701
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMult, Right: yyS[yypt-0].valExpr}
		}
	case 132:
		//line sql.y:705
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astDiv, Right: yyS[yypt-0].valExpr}
		}
	case 133:
		//line sql.y:709
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMod, Right: yyS[yypt-0].valExpr}
		}
	case 134:
		//line sql.y:713
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
	case 135:
		//line sql.y:728
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-2].str)}
		}
	case 136:
		//line sql.y:732
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 137:
		//line sql.y:736
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-4].str), Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 138:
		//line sql.y:740
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 139:
		//line sql.y:744
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 140:
		//line sql.y:750
		{
			yyVAL.str = "IF"
		}
	case 141:
		//line sql.y:754
		{
			yyVAL.str = "VALUES"
		}
	case 142:
		//line sql.y:760
		{
			yyVAL.byt = astUnaryPlus
		}
	case 143:
		//line sql.y:764
		{
			yyVAL.byt = astUnaryMinus
		}
	case 144:
		//line sql.y:768
		{
			yyVAL.byt = astTilda
		}
	case 145:
		//line sql.y:774
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 146:
		//line sql.y:779
		{
			yyVAL.valExpr = nil
		}
	case 147:
		//line sql.y:783
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 148:
		//line sql.y:789
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 149:
		//line sql.y:793
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 150:
		//line sql.y:799
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 151:
		//line sql.y:804
		{
			yyVAL.valExpr = nil
		}
	case 152:
		//line sql.y:808
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 153:
		//line sql.y:814
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].str}
		}
	case 154:
		//line sql.y:818
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 155:
		//line sql.y:824
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].str)
		}
	case 156:
		//line sql.y:828
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].str)
		}
	case 157:
		//line sql.y:832
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].str)
		}
	case 158:
		//line sql.y:836
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 159:
		//line sql.y:841
		{
			yyVAL.valExprs = nil
		}
	case 160:
		//line sql.y:845
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 161:
		//line sql.y:850
		{
			yyVAL.boolExpr = nil
		}
	case 162:
		//line sql.y:854
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 163:
		//line sql.y:859
		{
			yyVAL.orderBy = nil
		}
	case 164:
		//line sql.y:863
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 165:
		//line sql.y:869
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 166:
		//line sql.y:873
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 167:
		//line sql.y:879
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 168:
		//line sql.y:884
		{
			yyVAL.str = astAsc
		}
	case 169:
		//line sql.y:888
		{
			yyVAL.str = astAsc
		}
	case 170:
		//line sql.y:892
		{
			yyVAL.str = astDesc
		}
	case 171:
		//line sql.y:897
		{
			yyVAL.limit = nil
		}
	case 172:
		//line sql.y:901
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 173:
		//line sql.y:905
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 174:
		//line sql.y:909
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-0].valExpr, Rowcount: yyS[yypt-2].valExpr}
		}
	case 175:
		//line sql.y:914
		{
			yyVAL.str = ""
		}
	case 176:
		//line sql.y:918
		{
			yyVAL.str = astForUpdate
		}
	case 177:
		//line sql.y:922
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
	case 178:
		//line sql.y:935
		{
			yyVAL.columns = nil
		}
	case 179:
		//line sql.y:939
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 180:
		//line sql.y:945
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 181:
		//line sql.y:949
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 182:
		//line sql.y:954
		{
			yyVAL.updateExprs = nil
		}
	case 183:
		//line sql.y:958
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 184:
		//line sql.y:964
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 185:
		//line sql.y:968
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 186:
		//line sql.y:974
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 187:
		//line sql.y:979
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:981
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:984
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:986
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:989
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		//line sql.y:991
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		//line sql.y:995
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		//line sql.y:997
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		//line sql.y:999
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		//line sql.y:1001
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1003
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		//line sql.y:1006
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		//line sql.y:1008
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		//line sql.y:1011
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		//line sql.y:1013
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		//line sql.y:1016
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		//line sql.y:1018
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		//line sql.y:1022
		{
			yyVAL.str = strings.ToLower(yyS[yypt-0].str)
		}
	case 205:
		//line sql.y:1027
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
