//line sql.y:6
package parser

import __yyfmt__ "fmt"

//line sql.y:6
import (
	"strconv"
	"strings"
)

func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*tokenizer).parseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
	yylex.(*tokenizer).allowComments = allow
}

func forceEOF(yylex interface{}) {
	yylex.(*tokenizer).forceEOF = true
}

func parseInt(yylex yyLexer, s string) (int, bool) {
	i, err := strconv.Atoi(s)
	if err != nil {
		yylex.Error(err.Error())
		return -1, false
	}
	return i, true
}

//line sql.y:36
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
	columnType  ColumnType
	intVal      int
	intVal2     [2]int
	boolVal     bool
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

const yyNprod = 269
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 680

var yyAct = []int{

	107, 432, 67, 311, 387, 260, 179, 104, 434, 263,
	394, 177, 137, 115, 98, 439, 267, 217, 303, 254,
	462, 195, 93, 94, 105, 180, 3, 153, 154, 278,
	279, 280, 281, 282, 462, 283, 284, 462, 70, 72,
	15, 28, 29, 30, 31, 69, 483, 82, 309, 429,
	472, 85, 457, 58, 462, 462, 90, 462, 435, 148,
	315, 114, 89, 73, 120, 244, 80, 274, 309, 148,
	148, 482, 99, 78, 141, 246, 130, 131, 132, 373,
	134, 68, 136, 416, 138, 481, 369, 371, 480, 142,
	84, 50, 145, 146, 415, 51, 247, 150, 140, 479,
	71, 111, 112, 113, 414, 475, 463, 354, 461, 183,
	377, 314, 81, 118, 181, 53, 176, 178, 182, 308,
	298, 296, 52, 370, 186, 70, 245, 193, 70, 48,
	199, 378, 69, 189, 200, 69, 255, 116, 117, 289,
	198, 153, 154, 152, 121, 213, 129, 255, 430, 301,
	428, 125, 223, 199, 392, 234, 380, 135, 214, 215,
	99, 221, 166, 167, 168, 119, 204, 227, 225, 226,
	232, 233, 222, 236, 237, 238, 239, 240, 241, 242,
	243, 228, 39, 114, 40, 41, 120, 42, 43, 304,
	73, 270, 70, 70, 259, 248, 99, 99, 144, 69,
	261, 47, 268, 44, 413, 271, 46, 250, 252, 197,
	196, 265, 48, 153, 154, 262, 258, 127, 272, 412,
	410, 411, 71, 111, 112, 113, 393, 235, 304, 224,
	367, 183, 366, 221, 363, 118, 210, 288, 275, 364,
	361, 147, 291, 292, 57, 362, 54, 365, 55, 56,
	28, 29, 30, 31, 290, 196, 208, 127, 295, 116,
	117, 246, 460, 99, 458, 423, 121, 382, 220, 128,
	302, 355, 310, 300, 297, 450, 422, 219, 307, 449,
	448, 15, 306, 276, 447, 442, 191, 119, 211, 183,
	221, 221, 404, 403, 359, 360, 395, 425, 426, 251,
	400, 110, 398, 201, 187, 376, 114, 185, 184, 120,
	70, 122, 287, 379, 384, 148, 268, 383, 164, 165,
	166, 167, 168, 433, 385, 388, 192, 389, 127, 396,
	64, 73, 390, 71, 151, 124, 401, 402, 207, 209,
	206, 220, 374, 421, 372, 97, 111, 112, 113, 356,
	219, 212, 194, 139, 102, 405, 65, 286, 118, 161,
	162, 163, 164, 165, 166, 167, 168, 87, 406, 161,
	162, 163, 164, 165, 166, 167, 168, 86, 101, 73,
	477, 456, 116, 117, 95, 455, 381, 417, 79, 121,
	419, 15, 418, 431, 123, 63, 294, 126, 478, 88,
	437, 485, 438, 269, 440, 440, 436, 420, 61, 229,
	119, 230, 231, 202, 249, 257, 445, 443, 143, 59,
	441, 248, 312, 444, 110, 446, 45, 409, 454, 114,
	313, 264, 120, 452, 453, 388, 83, 408, 278, 279,
	280, 281, 282, 440, 283, 284, 358, 465, 70, 440,
	440, 440, 70, 466, 470, 261, 91, 92, 464, 69,
	471, 196, 473, 474, 467, 468, 469, 15, 97, 111,
	112, 113, 133, 77, 76, 75, 66, 102, 484, 32,
	451, 118, 15, 110, 33, 427, 486, 487, 114, 391,
	328, 120, 15, 16, 17, 18, 34, 35, 36, 37,
	38, 101, 327, 326, 325, 116, 117, 95, 320, 110,
	319, 318, 121, 316, 114, 266, 399, 120, 397, 459,
	74, 19, 203, 273, 205, 49, 190, 71, 111, 112,
	113, 476, 424, 119, 386, 407, 102, 357, 299, 188,
	118, 253, 109, 106, 108, 156, 160, 158, 159, 305,
	103, 256, 155, 71, 111, 112, 113, 100, 368, 218,
	101, 277, 102, 216, 116, 117, 118, 96, 285, 149,
	60, 121, 27, 62, 14, 13, 12, 11, 10, 20,
	9, 8, 7, 6, 5, 4, 101, 2, 1, 0,
	116, 117, 119, 172, 173, 174, 175, 121, 169, 170,
	171, 22, 23, 26, 24, 25, 21, 375, 0, 0,
	161, 162, 163, 164, 165, 166, 167, 168, 119, 0,
	0, 157, 161, 162, 163, 164, 165, 166, 167, 168,
	293, 0, 0, 161, 162, 163, 164, 165, 166, 167,
	168, 161, 162, 163, 164, 165, 166, 167, 168, 330,
	0, 331, 332, 333, 334, 335, 336, 337, 338, 339,
	340, 341, 321, 322, 323, 324, 342, 343, 344, 345,
	346, 347, 348, 349, 350, 351, 352, 353, 317, 329,
}
var yyPact = []int{

	487, -1000, -1000, 171, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 61, 81, -34, 0, -7, 124, 477, 401, -1000,
	-1000, -1000, 389, -1000, 365, 291, 467, 268, 266, -1000,
	466, 465, 464, -53, -64, -12, 266, -64, -1000, -32,
	266, -1000, 312, 302, -68, 266, -68, -68, -1000, -1000,
	403, -1000, 242, 291, 301, 44, 291, 174, -1000, 194,
	-1000, 39, -1000, -1000, -1000, 266, 266, 266, 463, 266,
	59, 266, -1000, 266, 288, -1000, -54, -1000, 266, 397,
	104, 266, 266, 232, -1000, -1000, 314, 36, 117, 523,
	-1000, 488, 462, -1000, -1000, -1000, 157, 234, 233, -1000,
	230, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 157, -1000, 252, 268, 287, 451, 268, 157, 266,
	-1000, -1000, -1000, 266, 229, 392, 71, -1000, -1000, 223,
	-1000, 286, -1000, -1000, 266, -1000, -1000, 203, 403, -1000,
	-1000, 266, 125, 488, 488, 157, 215, 387, 157, 157,
	129, 157, 157, 157, 157, 157, 157, 157, 157, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 523, -69, -8,
	-38, 523, -1000, 35, 280, 403, -1000, 477, 26, 542,
	386, 268, 268, 245, -1000, 418, 488, -1000, 542, -1000,
	-1000, -2, -1000, 97, 266, -1000, -61, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 200, 354, 292, 276,
	32, -1000, -1000, -1000, -1000, -1000, -1000, 542, -1000, 215,
	157, 157, 542, 534, -1000, 370, 216, 216, 216, 58,
	58, -1000, -1000, -1000, -1000, -1000, 157, -1000, 542, -1000,
	-13, 403, -14, 37, -1000, 488, 95, 215, 171, 134,
	-15, -1000, 418, 407, 416, 117, -23, -1000, 615, -17,
	266, -1000, -1000, 284, -1000, 435, 203, 203, -1000, -1000,
	156, 150, 163, 148, 146, -6, -1000, 279, -55, 277,
	-1000, 542, 511, 157, -1000, 542, -1000, -24, -1000, 18,
	-1000, 157, 45, -1000, 355, 184, -1000, -1000, -1000, 268,
	407, -1000, 157, 157, -1000, -2, 128, 222, 222, 228,
	226, -1000, -1000, -1000, -1000, 222, 222, -1000, -1000, 219,
	218, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 266, -1000, -1000, 425, 413, 354,
	126, -1000, 135, -1000, 120, -1000, -1000, -1000, -1000, -20,
	-30, -41, -1000, -1000, -1000, 157, 542, -1000, -1000, 542,
	157, 358, 215, -1000, -1000, 260, 182, -1000, 270, -1000,
	-1000, 17, -1000, 367, -1000, 256, -74, -74, 256, -1000,
	256, -1000, -1000, 266, 266, 211, -1000, 418, 488, 157,
	488, 210, -1000, -1000, 206, 205, 201, 542, 542, 473,
	-1000, 157, 157, 157, -1000, -1000, -1000, -1000, 353, -1000,
	349, -1000, -82, -1000, -1000, -1000, -1000, 181, 179, -26,
	-1000, -28, 266, 407, 117, 178, 117, 268, 266, 266,
	266, 268, 542, 542, -1000, -1000, -1000, -1000, 256, -84,
	256, -1000, 266, -1000, -29, 363, -35, -46, -49, -63,
	174, -88, -1000, -1000, -1000, -1000, -1000, 471, 379, -1000,
	-1000, -1000, -1000, -1000, -1000, 266, 266, -1000,
}
var yyPgo = []int{

	0, 588, 587, 25, 585, 584, 583, 582, 581, 580,
	578, 577, 576, 575, 574, 479, 573, 572, 570, 22,
	23, 569, 568, 567, 563, 17, 561, 559, 330, 558,
	15, 21, 14, 557, 552, 551, 550, 11, 24, 6,
	549, 544, 13, 543, 7, 542, 541, 19, 539, 538,
	537, 535, 9, 534, 4, 532, 3, 531, 526, 5,
	18, 2, 81, 525, 524, 523, 522, 8, 399, 388,
	403, 520, 10, 1, 519, 518, 516, 0, 515, 16,
	513, 511, 510, 508, 504, 503, 502, 490, 489, 485,
	12, 484,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 3, 3, 4, 4, 5, 6,
	7, 8, 9, 9, 9, 9, 9, 10, 10, 10,
	10, 78, 78, 79, 79, 80, 80, 80, 80, 80,
	80, 80, 80, 80, 80, 80, 80, 80, 80, 81,
	81, 81, 81, 81, 81, 82, 82, 82, 83, 83,
	84, 84, 85, 85, 86, 86, 86, 86, 87, 87,
	87, 87, 88, 88, 88, 89, 89, 89, 89, 89,
	11, 11, 11, 12, 13, 14, 14, 14, 14, 91,
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
	59, 59, 60, 60, 61, 61, 62, 68, 68, 69,
	69, 63, 63, 64, 64, 64, 64, 64, 65, 65,
	70, 70, 72, 72, 73, 75, 75, 76, 76, 74,
	74, 67, 67, 66, 66, 71, 71, 77, 90,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 12, 3, 7, 7, 8, 7,
	3, 3, 2, 3, 4, 4, 5, 8, 8, 4,
	4, 1, 3, 4, 6, 2, 3, 3, 2, 1,
	1, 1, 1, 2, 2, 1, 1, 4, 4, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 0, 1, 2, 0, 2, 1, 1, 2,
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
	0, 1, 0, 3, 1, 0, 5, 0, 4, 0,
	2, 0, 1, 0, 2, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, -14, 5, 6, 7, 8, 34,
	92, 119, 114, 115, 117, 118, 116, -17, 79, 80,
	81, 82, -15, -91, -15, -15, -15, -15, -15, 121,
	123, 124, 126, 127, 122, -70, 125, 120, 131, -63,
	125, 129, 122, 122, 122, 124, 125, 120, -3, 18,
	-18, 19, -16, 30, -28, 65, 9, -61, -62, -44,
	-77, 65, -77, 65, -71, 9, 9, 9, 126, -69,
	130, 124, -77, -69, 122, -77, 65, 65, -68, 130,
	-77, -68, -68, -19, -20, 104, -23, 65, -32, -37,
	-33, 98, 74, -36, -44, -38, -43, -77, -41, -45,
	21, 66, 67, 68, 26, -42, 102, 103, 78, 130,
	29, 109, 69, -28, 34, 107, -28, 83, 75, 107,
	-77, -77, -77, 9, -77, 98, -77, -90, -77, 65,
	-90, 128, -77, 21, 94, -77, -77, 9, 83, -21,
	-77, 20, 107, 96, 97, -34, 22, 98, 24, 25,
	23, 99, 100, 101, 102, 103, 104, 105, 106, 75,
	76, 77, 70, 71, 72, 73, -32, -37, -32, -39,
	-3, -37, -37, 74, 74, 74, -42, 74, -48, -37,
	-58, 34, 74, -61, 65, -31, 10, -62, -37, -77,
	-77, 74, 21, -66, 95, -64, 117, 115, 33, 116,
	13, 65, 65, -77, -90, -90, -24, -25, -27, 74,
	65, -42, -20, -77, 104, -32, -32, -37, -38, 22,
	24, 25, -37, -37, 26, 98, -37, -37, -37, -37,
	-37, -37, -37, -37, 134, 134, 83, 134, -37, 134,
	-19, 19, -19, -46, -47, 110, -35, 29, -3, -61,
	-59, -44, -31, -52, 13, -32, -78, -79, -77, -70,
	94, -77, -90, -65, 128, -31, 83, -26, 84, 85,
	86, 87, 88, 90, 91, -22, 65, 20, -25, 107,
	-38, -37, -37, 96, 26, -37, 134, -19, 134, -49,
	-47, 112, -32, -60, 94, -40, -38, -60, 134, 83,
	-52, -56, 15, 14, 134, 83, -80, 63, -81, -82,
	-83, 47, 48, 49, 50, -84, -85, -86, -87, 64,
	34, 36, 37, 38, 39, 40, 41, 42, 43, 44,
	45, 46, 51, 52, 53, 54, 55, 56, 57, 58,
	59, 60, 61, 62, 124, -77, 65, -50, 11, -25,
	-25, 84, 89, 84, 89, 84, 84, 84, -29, 92,
	129, 93, 65, 134, 65, 96, -37, 134, 113, -37,
	111, 31, 83, -44, -56, -37, -53, -54, -37, -90,
	-79, -88, 26, 98, -72, 74, -72, -75, 74, -76,
	74, -72, -72, 74, 74, -77, -90, -51, 12, 14,
	94, 95, 84, 84, 124, 124, 124, -37, -37, 32,
	-38, 83, 16, 83, -55, 27, 28, -89, 133, 32,
	131, 26, -73, 67, -67, 132, -67, -73, -73, -30,
	-77, -30, 74, -52, -32, -39, -32, 74, 74, 74,
	74, 7, -37, -37, -54, 32, 32, 134, 83, -74,
	83, 134, 83, 134, -30, -56, -59, -30, -30, -30,
	-61, -73, 134, -73, -77, 134, -57, 17, 35, 134,
	134, 134, 134, 134, 7, 22, -77, -77,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 89, 89, 89, 89, 89,
	89, 0, 250, 241, 0, 0, 0, 0, 93, 95,
	96, 97, 98, 91, 0, 0, 0, 0, 0, 22,
	265, 0, 0, 0, 239, 0, 0, 239, 251, 0,
	0, 242, 0, 0, 237, 0, 237, 237, 15, 94,
	0, 99, 90, 0, 0, 132, 0, 20, 234, 0,
	203, 267, 21, 267, 23, 0, 0, 0, 0, 0,
	0, 0, 268, 0, 0, 268, 0, 84, 0, 0,
	0, 0, 0, 0, 100, 102, 107, 267, 105, 106,
	142, 0, 0, 173, 174, 175, 0, 203, 0, 189,
	0, 205, 206, 207, 208, 169, 192, 193, 194, 190,
	191, 196, 92, 228, 0, 0, 140, 0, 0, 0,
	266, 24, 25, 0, 0, 0, 263, 29, 30, 0,
	82, 0, 85, 238, 0, 268, 268, 0, 0, 103,
	108, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 157,
	158, 159, 160, 161, 162, 163, 145, 0, 0, 0,
	0, 171, 184, 0, 0, 0, 156, 0, 0, 197,
	0, 0, 0, 140, 133, 213, 0, 235, 236, 204,
	26, 250, 240, 0, 0, 268, 248, 243, 244, 245,
	246, 247, 83, 86, 87, 88, 140, 110, 117, 0,
	129, 131, 101, 109, 104, 143, 144, 147, 148, 0,
	0, 0, 150, 0, 154, 0, 176, 177, 178, 179,
	180, 181, 182, 183, 146, 168, 0, 170, 171, 185,
	0, 0, 0, 201, 198, 0, 232, 0, 165, 232,
	0, 230, 213, 221, 0, 141, 0, 31, 0, 0,
	0, 264, 80, 0, 249, 209, 0, 0, 120, 121,
	0, 0, 0, 0, 0, 134, 118, 0, 0, 0,
	149, 151, 0, 0, 155, 172, 186, 0, 188, 0,
	199, 0, 0, 16, 0, 164, 166, 17, 229, 0,
	221, 19, 0, 0, 268, 250, 72, 252, 252, 255,
	257, 39, 40, 41, 42, 252, 252, 45, 46, 0,
	0, 49, 50, 51, 52, 53, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 70, 71, 0, 268, 81, 211, 0, 111,
	114, 122, 0, 124, 0, 126, 127, 128, 112, 0,
	0, 0, 119, 113, 130, 0, 152, 187, 195, 202,
	0, 0, 0, 231, 18, 222, 214, 215, 218, 27,
	32, 75, 73, 0, 35, 0, 261, 261, 0, 38,
	0, 43, 44, 0, 0, 0, 28, 213, 0, 0,
	0, 0, 123, 125, 0, 0, 0, 153, 200, 0,
	167, 0, 0, 0, 217, 219, 220, 33, 0, 77,
	78, 74, 0, 254, 36, 262, 37, 0, 259, 0,
	138, 0, 0, 221, 212, 210, 115, 0, 0, 0,
	0, 0, 223, 224, 216, 76, 79, 253, 0, 0,
	0, 47, 0, 48, 0, 225, 0, 0, 0, 0,
	233, 0, 258, 260, 139, 34, 14, 0, 0, 116,
	135, 136, 137, 256, 226, 0, 0, 227,
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
		//line sql.y:173
		{
			setParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:179
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
		//line sql.y:196
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].str2), Distinct: yyS[yypt-9].str, Exprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(astWhere, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(astHaving, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 15:
		//line sql.y:200
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 16:
		//line sql.y:206
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 17:
		//line sql.y:210
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
		//line sql.y:222
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].str2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:228
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].str2), Table: yyS[yypt-3].tableName, Where: NewWhere(astWhere, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 20:
		//line sql.y:234
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].str2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 21:
		//line sql.y:240
		{
			yyVAL.statement = &Use{Comments: Comments(yyS[yypt-1].str2), Name: yyS[yypt-0].str}
		}
	case 22:
		//line sql.y:246
		{
			yyVAL.statement = &ShowDatabases{}
		}
	case 23:
		//line sql.y:250
		{
			yyVAL.statement = &ShowTables{Name: yyS[yypt-0].str}
		}
	case 24:
		//line sql.y:254
		{
			yyVAL.statement = &ShowIndex{Name: yyS[yypt-0].str}
		}
	case 25:
		//line sql.y:258
		{
			yyVAL.statement = &ShowColumns{Name: yyS[yypt-0].str}
		}
	case 26:
		//line sql.y:262
		{
			yyVAL.statement = &ShowColumns{Name: yyS[yypt-0].str, Full: true}
		}
	case 27:
		//line sql.y:268
		{
			yyVAL.statement = &CreateTable{IfNotExists: yyS[yypt-5].boolVal, Name: yyS[yypt-4].str, Defs: yyS[yypt-2].tableDefs}
		}
	case 28:
		//line sql.y:272
		{
			yyVAL.statement = &CreateIndex{Name: yyS[yypt-4].str, TableName: yyS[yypt-1].str, Unique: yyS[yypt-6].boolVal}
		}
	case 29:
		//line sql.y:276
		{
			yyVAL.statement = &CreateView{Name: yyS[yypt-1].str}
		}
	case 30:
		//line sql.y:280
		{
			yyVAL.statement = &CreateDatabase{IfNotExists: yyS[yypt-1].boolVal, Name: yyS[yypt-0].str}
		}
	case 31:
		//line sql.y:286
		{
			yyVAL.tableDefs = TableDefs{yyS[yypt-0].tableDef}
		}
	case 32:
		//line sql.y:290
		{
			yyVAL.tableDefs = append(yyVAL.tableDefs, yyS[yypt-0].tableDef)
		}
	case 33:
		//line sql.y:296
		{
			yyVAL.tableDef = &ColumnTableDef{Name: yyS[yypt-3].str, Type: yyS[yypt-2].columnType, Null: NullType(yyS[yypt-1].intVal), PrimaryKey: yyS[yypt-0].intVal == 1, Unique: yyS[yypt-0].intVal == 2}
		}
	case 34:
		//line sql.y:300
		{
			yyVAL.tableDef = &IndexTableDef{Name: yyS[yypt-3].str, Unique: yyS[yypt-5].boolVal, Columns: yyS[yypt-1].str2}
		}
	case 35:
		//line sql.y:306
		{
			yyVAL.columnType = &BitType{N: yyS[yypt-0].intVal}
		}
	case 36:
		//line sql.y:308
		{
			yyVAL.columnType = &IntType{Name: yyS[yypt-2].str, N: yyS[yypt-1].intVal, Unsigned: yyS[yypt-0].boolVal}
		}
	case 37:
		//line sql.y:310
		{
			yyVAL.columnType = &FloatType{Name: yyS[yypt-2].str, N: yyS[yypt-1].intVal2[0], Prec: yyS[yypt-1].intVal2[1], Unsigned: yyS[yypt-0].boolVal}
		}
	case 38:
		//line sql.y:312
		{
			yyVAL.columnType = &DecimalType{Name: yyS[yypt-1].str, N: yyS[yypt-0].intVal2[0], Prec: yyS[yypt-0].intVal2[1]}
		}
	case 39:
		//line sql.y:314
		{
			yyVAL.columnType = &DateType{}
		}
	case 40:
		//line sql.y:316
		{
			yyVAL.columnType = &TimeType{}
		}
	case 41:
		//line sql.y:318
		{
			yyVAL.columnType = &DateTimeType{}
		}
	case 42:
		//line sql.y:320
		{
			yyVAL.columnType = &TimestampType{}
		}
	case 43:
		//line sql.y:322
		{
			yyVAL.columnType = &CharType{Name: yyS[yypt-1].str, N: yyS[yypt-0].intVal}
		}
	case 44:
		//line sql.y:324
		{
			yyVAL.columnType = &BinaryType{Name: yyS[yypt-1].str, N: yyS[yypt-0].intVal}
		}
	case 45:
		//line sql.y:326
		{
			yyVAL.columnType = &TextType{Name: yyS[yypt-0].str}
		}
	case 46:
		//line sql.y:328
		{
			yyVAL.columnType = &BlobType{Name: yyS[yypt-0].str}
		}
	case 47:
		//line sql.y:330
		{
			yyVAL.columnType = &EnumType{Vals: yyS[yypt-1].str2}
		}
	case 48:
		//line sql.y:332
		{
			yyVAL.columnType = &SetType{Vals: yyS[yypt-1].str2}
		}
	case 49:
		//line sql.y:336
		{
			yyVAL.str = astInt
		}
	case 50:
		//line sql.y:338
		{
			yyVAL.str = astTinyInt
		}
	case 51:
		//line sql.y:340
		{
			yyVAL.str = astSmallInt
		}
	case 52:
		//line sql.y:342
		{
			yyVAL.str = astMediumInt
		}
	case 53:
		//line sql.y:344
		{
			yyVAL.str = astBigInt
		}
	case 54:
		//line sql.y:346
		{
			yyVAL.str = astInteger
		}
	case 55:
		//line sql.y:350
		{
			yyVAL.str = astReal
		}
	case 56:
		//line sql.y:352
		{
			yyVAL.str = astDouble
		}
	case 57:
		//line sql.y:354
		{
			yyVAL.str = astFloat
		}
	case 58:
		//line sql.y:358
		{
			yyVAL.str = astDecimal
		}
	case 59:
		//line sql.y:360
		{
			yyVAL.str = astNumeric
		}
	case 60:
		//line sql.y:364
		{
			yyVAL.str = astChar
		}
	case 61:
		//line sql.y:366
		{
			yyVAL.str = astVarChar
		}
	case 62:
		//line sql.y:370
		{
			yyVAL.str = astBinary
		}
	case 63:
		//line sql.y:372
		{
			yyVAL.str = astVarBinary
		}
	case 64:
		//line sql.y:376
		{
			yyVAL.str = astText
		}
	case 65:
		//line sql.y:378
		{
			yyVAL.str = astTinyText
		}
	case 66:
		//line sql.y:380
		{
			yyVAL.str = astMediumText
		}
	case 67:
		//line sql.y:382
		{
			yyVAL.str = astLongText
		}
	case 68:
		//line sql.y:386
		{
			yyVAL.str = astBlob
		}
	case 69:
		//line sql.y:388
		{
			yyVAL.str = astTinyBlob
		}
	case 70:
		//line sql.y:390
		{
			yyVAL.str = astMediumBlob
		}
	case 71:
		//line sql.y:392
		{
			yyVAL.str = astLongBlob
		}
	case 72:
		//line sql.y:395
		{
			yyVAL.intVal = int(SilentNull)
		}
	case 73:
		//line sql.y:397
		{
			yyVAL.intVal = int(Null)
		}
	case 74:
		//line sql.y:399
		{
			yyVAL.intVal = int(NotNull)
		}
	case 75:
		//line sql.y:402
		{
			yyVAL.intVal = 0
		}
	case 76:
		//line sql.y:404
		{
			yyVAL.intVal = 1
		}
	case 77:
		//line sql.y:406
		{
			yyVAL.intVal = 1
		}
	case 78:
		//line sql.y:408
		{
			yyVAL.intVal = 2
		}
	case 79:
		//line sql.y:410
		{
			yyVAL.intVal = 2
		}
	case 80:
		//line sql.y:414
		{
			yyVAL.statement = &AlterTable{Name: yyS[yypt-2].str}
		}
	case 81:
		//line sql.y:418
		{
			// Change this to a rename statement
			yyVAL.statement = &RenameTable{Name: yyS[yypt-3].str, NewName: yyS[yypt-0].str}
		}
	case 82:
		//line sql.y:423
		{
			yyVAL.statement = &AlterView{Name: yyS[yypt-1].str}
		}
	case 83:
		//line sql.y:429
		{
			yyVAL.statement = &RenameTable{Name: yyS[yypt-2].str, NewName: yyS[yypt-0].str}
		}
	case 84:
		//line sql.y:435
		{
			yyVAL.statement = &TruncateTable{Name: yyS[yypt-0].str}
		}
	case 85:
		//line sql.y:441
		{
			yyVAL.statement = &DropTable{Name: yyS[yypt-0].str, IfExists: yyS[yypt-1].boolVal}
		}
	case 86:
		//line sql.y:445
		{
			yyVAL.statement = &DropIndex{Name: yyS[yypt-2].str, TableName: yyS[yypt-0].str}
		}
	case 87:
		//line sql.y:449
		{
			yyVAL.statement = &DropView{Name: yyS[yypt-1].str, IfExists: yyS[yypt-2].boolVal}
		}
	case 88:
		//line sql.y:453
		{
			yyVAL.statement = &DropDatabase{Name: yyS[yypt-1].str, IfExists: yyS[yypt-2].boolVal}
		}
	case 89:
		//line sql.y:458
		{
			setAllowComments(yylex, true)
		}
	case 90:
		//line sql.y:462
		{
			yyVAL.str2 = yyS[yypt-0].str2
			setAllowComments(yylex, false)
		}
	case 91:
		//line sql.y:468
		{
			yyVAL.str2 = nil
		}
	case 92:
		//line sql.y:472
		{
			yyVAL.str2 = append(yyS[yypt-1].str2, yyS[yypt-0].str)
		}
	case 93:
		//line sql.y:478
		{
			yyVAL.str = astUnion
		}
	case 94:
		//line sql.y:482
		{
			yyVAL.str = astUnionAll
		}
	case 95:
		//line sql.y:486
		{
			yyVAL.str = astSetMinus
		}
	case 96:
		//line sql.y:490
		{
			yyVAL.str = astExcept
		}
	case 97:
		//line sql.y:494
		{
			yyVAL.str = astIntersect
		}
	case 98:
		//line sql.y:499
		{
			yyVAL.str = ""
		}
	case 99:
		//line sql.y:503
		{
			yyVAL.str = astDistinct
		}
	case 100:
		//line sql.y:509
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 101:
		//line sql.y:513
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 102:
		//line sql.y:519
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 103:
		//line sql.y:523
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].str}
		}
	case 104:
		//line sql.y:527
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].str}
		}
	case 105:
		//line sql.y:533
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 106:
		//line sql.y:537
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 107:
		//line sql.y:542
		{
			yyVAL.str = ""
		}
	case 108:
		//line sql.y:546
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 109:
		//line sql.y:550
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 110:
		//line sql.y:556
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 111:
		//line sql.y:560
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 112:
		//line sql.y:566
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].str, Hints: yyS[yypt-0].indexHints}
		}
	case 113:
		//line sql.y:570
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 114:
		//line sql.y:574
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 115:
		//line sql.y:578
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, Cond: &OnJoinCond{yyS[yypt-0].boolExpr}}
		}
	case 116:
		//line sql.y:582
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-6].tableExpr, Join: yyS[yypt-5].str, RightExpr: yyS[yypt-4].tableExpr, Cond: &UsingJoinCond{yyS[yypt-1].columns}}
		}
	case 117:
		//line sql.y:587
		{
			yyVAL.str = ""
		}
	case 118:
		//line sql.y:591
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 119:
		//line sql.y:595
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 120:
		//line sql.y:601
		{
			yyVAL.str = astJoin
		}
	case 121:
		//line sql.y:605
		{
			yyVAL.str = astStraightJoin
		}
	case 122:
		//line sql.y:609
		{
			yyVAL.str = astLeftJoin
		}
	case 123:
		//line sql.y:613
		{
			yyVAL.str = astLeftJoin
		}
	case 124:
		//line sql.y:617
		{
			yyVAL.str = astRightJoin
		}
	case 125:
		//line sql.y:621
		{
			yyVAL.str = astRightJoin
		}
	case 126:
		//line sql.y:625
		{
			yyVAL.str = astJoin
		}
	case 127:
		//line sql.y:629
		{
			yyVAL.str = astCrossJoin
		}
	case 128:
		//line sql.y:633
		{
			yyVAL.str = astNaturalJoin
		}
	case 129:
		//line sql.y:639
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].str}
		}
	case 130:
		//line sql.y:643
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 131:
		//line sql.y:647
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 132:
		//line sql.y:653
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].str}
		}
	case 133:
		//line sql.y:657
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 134:
		//line sql.y:662
		{
			yyVAL.indexHints = nil
		}
	case 135:
		//line sql.y:666
		{
			yyVAL.indexHints = &IndexHints{Type: astUse, Indexes: yyS[yypt-1].str2}
		}
	case 136:
		//line sql.y:670
		{
			yyVAL.indexHints = &IndexHints{Type: astIgnore, Indexes: yyS[yypt-1].str2}
		}
	case 137:
		//line sql.y:674
		{
			yyVAL.indexHints = &IndexHints{Type: astForce, Indexes: yyS[yypt-1].str2}
		}
	case 138:
		//line sql.y:680
		{
			yyVAL.str2 = []string{yyS[yypt-0].str}
		}
	case 139:
		//line sql.y:684
		{
			yyVAL.str2 = append(yyS[yypt-2].str2, yyS[yypt-0].str)
		}
	case 140:
		//line sql.y:689
		{
			yyVAL.boolExpr = nil
		}
	case 141:
		//line sql.y:693
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 142:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 143:
		//line sql.y:700
		{
			yyVAL.boolExpr = &AndExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 144:
		//line sql.y:704
		{
			yyVAL.boolExpr = &OrExpr{Op: string(yyS[yypt-1].str), Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 145:
		//line sql.y:708
		{
			yyVAL.boolExpr = &NotExpr{Op: string(yyS[yypt-1].str), Expr: yyS[yypt-0].boolExpr}
		}
	case 146:
		//line sql.y:712
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 147:
		//line sql.y:718
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 148:
		//line sql.y:722
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astIn, Right: yyS[yypt-0].tuple}
		}
	case 149:
		//line sql.y:726
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotIn, Right: yyS[yypt-0].tuple}
		}
	case 150:
		//line sql.y:730
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: astLike, Right: yyS[yypt-0].valExpr}
		}
	case 151:
		//line sql.y:734
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: astNotLike, Right: yyS[yypt-0].valExpr}
		}
	case 152:
		//line sql.y:738
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: astBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 153:
		//line sql.y:742
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: astNotBetween, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 154:
		//line sql.y:746
		{
			yyVAL.boolExpr = &NullCheck{Operator: astNull, Expr: yyS[yypt-2].valExpr}
		}
	case 155:
		//line sql.y:750
		{
			yyVAL.boolExpr = &NullCheck{Operator: astNotNull, Expr: yyS[yypt-3].valExpr}
		}
	case 156:
		//line sql.y:754
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 157:
		//line sql.y:760
		{
			yyVAL.str = astEQ
		}
	case 158:
		//line sql.y:764
		{
			yyVAL.str = astLT
		}
	case 159:
		//line sql.y:768
		{
			yyVAL.str = astGT
		}
	case 160:
		//line sql.y:772
		{
			yyVAL.str = astLE
		}
	case 161:
		//line sql.y:776
		{
			yyVAL.str = astGE
		}
	case 162:
		//line sql.y:780
		{
			yyVAL.str = astNE
		}
	case 163:
		//line sql.y:784
		{
			yyVAL.str = astNSE
		}
	case 164:
		//line sql.y:790
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 165:
		//line sql.y:794
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 166:
		//line sql.y:800
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 167:
		//line sql.y:804
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 168:
		//line sql.y:810
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 169:
		//line sql.y:814
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 170:
		//line sql.y:820
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 171:
		//line sql.y:826
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 172:
		//line sql.y:830
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 173:
		//line sql.y:836
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 174:
		//line sql.y:840
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 175:
		//line sql.y:844
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 176:
		//line sql.y:848
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitand, Right: yyS[yypt-0].valExpr}
		}
	case 177:
		//line sql.y:852
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitor, Right: yyS[yypt-0].valExpr}
		}
	case 178:
		//line sql.y:856
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astBitxor, Right: yyS[yypt-0].valExpr}
		}
	case 179:
		//line sql.y:860
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astPlus, Right: yyS[yypt-0].valExpr}
		}
	case 180:
		//line sql.y:864
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMinus, Right: yyS[yypt-0].valExpr}
		}
	case 181:
		//line sql.y:868
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMult, Right: yyS[yypt-0].valExpr}
		}
	case 182:
		//line sql.y:872
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astDiv, Right: yyS[yypt-0].valExpr}
		}
	case 183:
		//line sql.y:876
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: astMod, Right: yyS[yypt-0].valExpr}
		}
	case 184:
		//line sql.y:880
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
	case 185:
		//line sql.y:895
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-2].str)}
		}
	case 186:
		//line sql.y:899
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 187:
		//line sql.y:903
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-4].str), Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 188:
		//line sql.y:907
		{
			yyVAL.valExpr = &FuncExpr{Name: strings.ToUpper(yyS[yypt-3].str), Exprs: yyS[yypt-1].selectExprs}
		}
	case 189:
		//line sql.y:911
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 190:
		//line sql.y:917
		{
			yyVAL.str = "IF"
		}
	case 191:
		//line sql.y:921
		{
			yyVAL.str = "VALUES"
		}
	case 192:
		//line sql.y:927
		{
			yyVAL.byt = astUnaryPlus
		}
	case 193:
		//line sql.y:931
		{
			yyVAL.byt = astUnaryMinus
		}
	case 194:
		//line sql.y:935
		{
			yyVAL.byt = astTilda
		}
	case 195:
		//line sql.y:941
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 196:
		//line sql.y:946
		{
			yyVAL.valExpr = nil
		}
	case 197:
		//line sql.y:950
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 198:
		//line sql.y:956
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 199:
		//line sql.y:960
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 200:
		//line sql.y:966
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 201:
		//line sql.y:971
		{
			yyVAL.valExpr = nil
		}
	case 202:
		//line sql.y:975
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 203:
		//line sql.y:981
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].str}
		}
	case 204:
		//line sql.y:985
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].str, Name: yyS[yypt-0].str}
		}
	case 205:
		//line sql.y:991
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].str)
		}
	case 206:
		//line sql.y:995
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].str)
		}
	case 207:
		//line sql.y:999
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].str)
		}
	case 208:
		//line sql.y:1003
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 209:
		//line sql.y:1008
		{
			yyVAL.valExprs = nil
		}
	case 210:
		//line sql.y:1012
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 211:
		//line sql.y:1017
		{
			yyVAL.boolExpr = nil
		}
	case 212:
		//line sql.y:1021
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 213:
		//line sql.y:1026
		{
			yyVAL.orderBy = nil
		}
	case 214:
		//line sql.y:1030
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 215:
		//line sql.y:1036
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 216:
		//line sql.y:1040
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 217:
		//line sql.y:1046
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 218:
		//line sql.y:1051
		{
			yyVAL.str = astAsc
		}
	case 219:
		//line sql.y:1055
		{
			yyVAL.str = astAsc
		}
	case 220:
		//line sql.y:1059
		{
			yyVAL.str = astDesc
		}
	case 221:
		//line sql.y:1064
		{
			yyVAL.limit = nil
		}
	case 222:
		//line sql.y:1068
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 223:
		//line sql.y:1072
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 224:
		//line sql.y:1076
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-0].valExpr, Rowcount: yyS[yypt-2].valExpr}
		}
	case 225:
		//line sql.y:1081
		{
			yyVAL.str = ""
		}
	case 226:
		//line sql.y:1085
		{
			yyVAL.str = astForUpdate
		}
	case 227:
		//line sql.y:1089
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
	case 228:
		//line sql.y:1102
		{
			yyVAL.columns = nil
		}
	case 229:
		//line sql.y:1106
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 230:
		//line sql.y:1112
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 231:
		//line sql.y:1116
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 232:
		//line sql.y:1121
		{
			yyVAL.updateExprs = nil
		}
	case 233:
		//line sql.y:1125
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 234:
		//line sql.y:1131
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 235:
		//line sql.y:1135
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 236:
		//line sql.y:1141
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 237:
		//line sql.y:1146
		{
			yyVAL.boolVal = false
		}
	case 238:
		//line sql.y:1148
		{
			yyVAL.boolVal = true
		}
	case 239:
		//line sql.y:1151
		{
			yyVAL.boolVal = false
		}
	case 240:
		//line sql.y:1153
		{
			yyVAL.boolVal = true
		}
	case 241:
		//line sql.y:1156
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		//line sql.y:1158
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		//line sql.y:1162
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		//line sql.y:1164
		{
			yyVAL.empty = struct{}{}
		}
	case 245:
		//line sql.y:1166
		{
			yyVAL.empty = struct{}{}
		}
	case 246:
		//line sql.y:1168
		{
			yyVAL.empty = struct{}{}
		}
	case 247:
		//line sql.y:1170
		{
			yyVAL.empty = struct{}{}
		}
	case 248:
		//line sql.y:1173
		{
			yyVAL.empty = struct{}{}
		}
	case 249:
		//line sql.y:1175
		{
			yyVAL.empty = struct{}{}
		}
	case 250:
		//line sql.y:1178
		{
			yyVAL.boolVal = false
		}
	case 251:
		//line sql.y:1180
		{
			yyVAL.boolVal = true
		}
	case 252:
		//line sql.y:1183
		{
			yyVAL.intVal = -1
		}
	case 253:
		//line sql.y:1185
		{
			yyVAL.intVal = yyS[yypt-1].intVal
		}
	case 254:
		//line sql.y:1189
		{
			if i, ok := parseInt(yylex, yyS[yypt-0].str); !ok {
				return 1
			} else {
				yyVAL.intVal = i
			}
		}
	case 255:
		//line sql.y:1198
		{
			yyVAL.intVal2[0], yyVAL.intVal2[1] = -1, -1
		}
	case 256:
		//line sql.y:1200
		{
			yyVAL.intVal2[0], yyVAL.intVal2[1] = yyS[yypt-3].intVal, yyS[yypt-1].intVal
		}
	case 257:
		//line sql.y:1203
		{
			yyVAL.intVal2[0], yyVAL.intVal2[1] = -1, -1
		}
	case 258:
		//line sql.y:1205
		{
			yyVAL.intVal2[0], yyVAL.intVal2[1] = yyS[yypt-2].intVal, yyS[yypt-1].intVal
		}
	case 259:
		//line sql.y:1208
		{
			yyVAL.intVal = -1
		}
	case 260:
		//line sql.y:1210
		{
			yyVAL.intVal = yyS[yypt-0].intVal
		}
	case 261:
		//line sql.y:1213
		{
			yyVAL.boolVal = false
		}
	case 262:
		//line sql.y:1215
		{
			yyVAL.boolVal = true
		}
	case 263:
		//line sql.y:1218
		{
			yyVAL.empty = struct{}{}
		}
	case 264:
		//line sql.y:1220
		{
			yyVAL.empty = struct{}{}
		}
	case 265:
		//line sql.y:1223
		{
			yyVAL.str = ""
		}
	case 266:
		//line sql.y:1225
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 267:
		//line sql.y:1229
		{
			yyVAL.str = strings.ToLower(yyS[yypt-0].str)
		}
	case 268:
		//line sql.y:1232
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
