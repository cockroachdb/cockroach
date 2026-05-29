// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgdate

import "strings"

// PGTimezoneAbbrev is a row in CockroachDB's mirror of PostgreSQL's
// pg_timezone_abbrevs view. Abbreviations are fixed-offset names like EST,
// PST, EAT, etc. UTCOffsetSecs is positive east of UTC. IsDST is true for
// daylight-savings abbreviations such as EDT and PDT.
type PGTimezoneAbbrev struct {
	UTCOffsetSecs int32
	IsDST         bool
}

// pgTimezoneAbbrevs is the abbreviation table, keyed by upper-case
// abbreviation. It mirrors the fixed-offset entries in PostgreSQL's
// src/timezone/tznames/Default at tag REL_18_3. PostgreSQL's "DYNTZ" entries
// (rows whose value is an IANA zone name like America/Argentina/Buenos_Aires)
// are intentionally omitted: those resolve to a session-zone-dependent offset
// in PostgreSQL via TimeZoneAbbrevIsKnown, which CockroachDB does not
// emulate. They will continue to fall through to LoadLocation, which is the
// behavior that existed before this table was introduced.
var pgTimezoneAbbrevs = map[string]PGTimezoneAbbrev{
	"ACDT":   {UTCOffsetSecs: 37800, IsDST: true},
	"ACSST":  {UTCOffsetSecs: 37800, IsDST: true},
	"ACST":   {UTCOffsetSecs: 34200, IsDST: false},
	"ACT":    {UTCOffsetSecs: -18000, IsDST: false},
	"ACWST":  {UTCOffsetSecs: 31500, IsDST: false},
	"ADT":    {UTCOffsetSecs: -10800, IsDST: true},
	"AEDT":   {UTCOffsetSecs: 39600, IsDST: true},
	"AESST":  {UTCOffsetSecs: 39600, IsDST: true},
	"AEST":   {UTCOffsetSecs: 36000, IsDST: false},
	"AFT":    {UTCOffsetSecs: 16200, IsDST: false},
	"AKDT":   {UTCOffsetSecs: -28800, IsDST: true},
	"AKST":   {UTCOffsetSecs: -32400, IsDST: false},
	"ALMST":  {UTCOffsetSecs: 25200, IsDST: true},
	"ALMT":   {UTCOffsetSecs: 21600, IsDST: false},
	"AMT":    {UTCOffsetSecs: -14400, IsDST: false},
	"AST":    {UTCOffsetSecs: -14400, IsDST: false},
	"AWSST":  {UTCOffsetSecs: 32400, IsDST: true},
	"AWST":   {UTCOffsetSecs: 28800, IsDST: false},
	"AZOST":  {UTCOffsetSecs: 0, IsDST: true},
	"AZOT":   {UTCOffsetSecs: -3600, IsDST: false},
	"BDST":   {UTCOffsetSecs: 7200, IsDST: true},
	"BDT":    {UTCOffsetSecs: 21600, IsDST: false},
	"BNT":    {UTCOffsetSecs: 28800, IsDST: false},
	"BORT":   {UTCOffsetSecs: 28800, IsDST: false},
	"BOT":    {UTCOffsetSecs: -14400, IsDST: false},
	"BRA":    {UTCOffsetSecs: -10800, IsDST: false},
	"BRST":   {UTCOffsetSecs: -7200, IsDST: true},
	"BRT":    {UTCOffsetSecs: -10800, IsDST: false},
	"BST":    {UTCOffsetSecs: 3600, IsDST: true},
	"BTT":    {UTCOffsetSecs: 21600, IsDST: false},
	"CADT":   {UTCOffsetSecs: 37800, IsDST: true},
	"CAST":   {UTCOffsetSecs: 34200, IsDST: false},
	"CCT":    {UTCOffsetSecs: 28800, IsDST: false},
	"CDT":    {UTCOffsetSecs: -18000, IsDST: true},
	"CEST":   {UTCOffsetSecs: 7200, IsDST: true},
	"CET":    {UTCOffsetSecs: 3600, IsDST: false},
	"CETDST": {UTCOffsetSecs: 7200, IsDST: true},
	"CHADT":  {UTCOffsetSecs: 49500, IsDST: true},
	"CHAST":  {UTCOffsetSecs: 45900, IsDST: false},
	"CHUT":   {UTCOffsetSecs: 36000, IsDST: false},
	"CLST":   {UTCOffsetSecs: -10800, IsDST: true},
	"COT":    {UTCOffsetSecs: -18000, IsDST: false},
	"CST":    {UTCOffsetSecs: -21600, IsDST: false},
	"CXT":    {UTCOffsetSecs: 25200, IsDST: false},
	"DDUT":   {UTCOffsetSecs: 36000, IsDST: false},
	"EAT":    {UTCOffsetSecs: 10800, IsDST: false},
	"EDT":    {UTCOffsetSecs: -14400, IsDST: true},
	"EEST":   {UTCOffsetSecs: 10800, IsDST: true},
	"EET":    {UTCOffsetSecs: 7200, IsDST: false},
	"EETDST": {UTCOffsetSecs: 10800, IsDST: true},
	"EGST":   {UTCOffsetSecs: 0, IsDST: true},
	"EGT":    {UTCOffsetSecs: -3600, IsDST: false},
	"EST":    {UTCOffsetSecs: -18000, IsDST: false},
	"FET":    {UTCOffsetSecs: 10800, IsDST: false},
	"FJST":   {UTCOffsetSecs: 46800, IsDST: true},
	"FJT":    {UTCOffsetSecs: 43200, IsDST: false},
	"FNST":   {UTCOffsetSecs: -3600, IsDST: true},
	"FNT":    {UTCOffsetSecs: -7200, IsDST: false},
	"GALT":   {UTCOffsetSecs: -21600, IsDST: false},
	"GAMT":   {UTCOffsetSecs: -32400, IsDST: false},
	"GFT":    {UTCOffsetSecs: -10800, IsDST: false},
	"GILT":   {UTCOffsetSecs: 43200, IsDST: false},
	"GMT":    {UTCOffsetSecs: 0, IsDST: false},
	"HKT":    {UTCOffsetSecs: 28800, IsDST: false},
	"HST":    {UTCOffsetSecs: -36000, IsDST: false},
	"ICT":    {UTCOffsetSecs: 25200, IsDST: false},
	"IDT":    {UTCOffsetSecs: 10800, IsDST: true},
	"IRT":    {UTCOffsetSecs: 12600, IsDST: false},
	"IST":    {UTCOffsetSecs: 7200, IsDST: false},
	"JAYT":   {UTCOffsetSecs: 32400, IsDST: false},
	"JST":    {UTCOffsetSecs: 32400, IsDST: false},
	"KDT":    {UTCOffsetSecs: 36000, IsDST: true},
	"KGST":   {UTCOffsetSecs: 21600, IsDST: true},
	"KST":    {UTCOffsetSecs: 32400, IsDST: false},
	"LHST":   {UTCOffsetSecs: 37800, IsDST: false},
	"LIGT":   {UTCOffsetSecs: 36000, IsDST: false},
	"MART":   {UTCOffsetSecs: -34200, IsDST: false},
	"MDT":    {UTCOffsetSecs: -21600, IsDST: true},
	"MEST":   {UTCOffsetSecs: 7200, IsDST: true},
	"MESZ":   {UTCOffsetSecs: 7200, IsDST: true},
	"MET":    {UTCOffsetSecs: 3600, IsDST: false},
	"METDST": {UTCOffsetSecs: 7200, IsDST: true},
	"MEZ":    {UTCOffsetSecs: 3600, IsDST: false},
	"MHT":    {UTCOffsetSecs: 43200, IsDST: false},
	"MMT":    {UTCOffsetSecs: 23400, IsDST: false},
	"MPT":    {UTCOffsetSecs: 36000, IsDST: false},
	"MSD":    {UTCOffsetSecs: 14400, IsDST: true},
	"MST":    {UTCOffsetSecs: -25200, IsDST: false},
	"MUST":   {UTCOffsetSecs: 18000, IsDST: true},
	"MUT":    {UTCOffsetSecs: 14400, IsDST: false},
	"MVT":    {UTCOffsetSecs: 18000, IsDST: false},
	"MYT":    {UTCOffsetSecs: 28800, IsDST: false},
	"NDT":    {UTCOffsetSecs: -9000, IsDST: true},
	"NFT":    {UTCOffsetSecs: -12600, IsDST: false},
	"NPT":    {UTCOffsetSecs: 20700, IsDST: false},
	"NST":    {UTCOffsetSecs: -12600, IsDST: false},
	"NZDT":   {UTCOffsetSecs: 46800, IsDST: true},
	"NZST":   {UTCOffsetSecs: 43200, IsDST: false},
	"NZT":    {UTCOffsetSecs: 43200, IsDST: false},
	"PDT":    {UTCOffsetSecs: -25200, IsDST: true},
	"PET":    {UTCOffsetSecs: -18000, IsDST: false},
	"PGT":    {UTCOffsetSecs: 36000, IsDST: false},
	"PHT":    {UTCOffsetSecs: 28800, IsDST: false},
	"PKST":   {UTCOffsetSecs: 21600, IsDST: true},
	"PKT":    {UTCOffsetSecs: 18000, IsDST: false},
	"PMDT":   {UTCOffsetSecs: -7200, IsDST: true},
	"PMST":   {UTCOffsetSecs: -10800, IsDST: false},
	"PONT":   {UTCOffsetSecs: 39600, IsDST: false},
	"PST":    {UTCOffsetSecs: -28800, IsDST: false},
	"PWT":    {UTCOffsetSecs: 32400, IsDST: false},
	"PYST":   {UTCOffsetSecs: -10800, IsDST: true},
	"RET":    {UTCOffsetSecs: 14400, IsDST: false},
	"SADT":   {UTCOffsetSecs: 37800, IsDST: true},
	"SAST":   {UTCOffsetSecs: 7200, IsDST: false},
	"SCT":    {UTCOffsetSecs: 14400, IsDST: false},
	"TAHT":   {UTCOffsetSecs: -36000, IsDST: false},
	"TFT":    {UTCOffsetSecs: 18000, IsDST: false},
	"TJT":    {UTCOffsetSecs: 18000, IsDST: false},
	"TOT":    {UTCOffsetSecs: 46800, IsDST: false},
	"TRUT":   {UTCOffsetSecs: 36000, IsDST: false},
	"TVT":    {UTCOffsetSecs: 43200, IsDST: false},
	"UCT":    {UTCOffsetSecs: 0, IsDST: false},
	"ULAST":  {UTCOffsetSecs: 32400, IsDST: true},
	"UT":     {UTCOffsetSecs: 0, IsDST: false},
	"UTC":    {UTCOffsetSecs: 0, IsDST: false},
	"UYST":   {UTCOffsetSecs: -7200, IsDST: true},
	"UYT":    {UTCOffsetSecs: -10800, IsDST: false},
	"UZST":   {UTCOffsetSecs: 21600, IsDST: true},
	"UZT":    {UTCOffsetSecs: 18000, IsDST: false},
	"VUT":    {UTCOffsetSecs: 39600, IsDST: false},
	"WADT":   {UTCOffsetSecs: 28800, IsDST: true},
	"WAKT":   {UTCOffsetSecs: 43200, IsDST: false},
	"WAST":   {UTCOffsetSecs: 25200, IsDST: false},
	"WAT":    {UTCOffsetSecs: 3600, IsDST: false},
	"WDT":    {UTCOffsetSecs: 32400, IsDST: true},
	"WET":    {UTCOffsetSecs: 0, IsDST: false},
	"WETDST": {UTCOffsetSecs: 3600, IsDST: true},
	"WFT":    {UTCOffsetSecs: 43200, IsDST: false},
	"WGST":   {UTCOffsetSecs: -7200, IsDST: true},
	"WGT":    {UTCOffsetSecs: -10800, IsDST: false},
	"XJT":    {UTCOffsetSecs: 21600, IsDST: false},
	"YAPT":   {UTCOffsetSecs: 36000, IsDST: false},
	"YEKST":  {UTCOffsetSecs: 21600, IsDST: true},
	"Z":      {UTCOffsetSecs: 0, IsDST: false},
	"ZULU":   {UTCOffsetSecs: 0, IsDST: false},
}

// LookupPGTimezoneAbbrev returns the entry for an abbreviation matched
// case-insensitively. The second return value is false if the input is not a
// known abbreviation.
func LookupPGTimezoneAbbrev(s string) (PGTimezoneAbbrev, bool) {
	abbrev, ok := pgTimezoneAbbrevs[strings.ToUpper(s)]
	return abbrev, ok
}

// PGTimezoneAbbrevs returns the abbreviation table for use by callers that
// need to enumerate it (for example, the pg_catalog.pg_timezone_abbrevs
// virtual table). The map is returned by reference and must not be mutated.
func PGTimezoneAbbrevs() map[string]PGTimezoneAbbrev {
	return pgTimezoneAbbrevs
}
