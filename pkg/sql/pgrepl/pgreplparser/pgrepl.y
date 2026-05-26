%{
// Adapted from https://github.com/postgres/postgres/blob/4327f6c7480fea9348ea6825a9d38a71b2ef8785/src/backend/replication/repl_gram.y
/*-------------------------------------------------------------------------
 *
 * repl_gram.y        - Parser for the replication commands
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/replication/repl_gram.y
 *
 *-------------------------------------------------------------------------
 */

package pgreplparser

import (
  "github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
  "github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgrepltree"
  "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
  "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)
%}

%{

func (s *pgreplSymType) ID() int32 {
  return s.id
}

func (s *pgreplSymType) SetID(id int32) {
  s.id = id
}

func (s *pgreplSymType) Pos() int32 {
  return s.pos
}

func (s *pgreplSymType) SetPos(pos int32) {
  s.pos = pos
}

func (s *pgreplSymType) Str() string {
  return s.str
}

func (s *pgreplSymType) SetStr(str string) {
  s.str = str
}

func (s *pgreplSymType) UnionVal() interface{} {
  return s.union.val
}

func (s *pgreplSymType) SetUnionVal(val interface{}) {
  s.union.val = val
}

type pgreplSymUnion struct {
  val interface{}
}

func (u *pgreplSymUnion) replicationStatement() pgrepltree.ReplicationStatement {
  return u.val.(pgrepltree.ReplicationStatement)
}

func (u *pgreplSymUnion) identifySystem() *pgrepltree.IdentifySystem {
  return u.val.(*pgrepltree.IdentifySystem)
}

func (u *pgreplSymUnion) readReplicationSlot() *pgrepltree.ReadReplicationSlot {
  return u.val.(*pgrepltree.ReadReplicationSlot)
}

func (u *pgreplSymUnion) createReplicationSlot() *pgrepltree.CreateReplicationSlot {
  return u.val.(*pgrepltree.CreateReplicationSlot)
}

func (u *pgreplSymUnion) dropReplicationSlot() *pgrepltree.DropReplicationSlot {
  return u.val.(*pgrepltree.DropReplicationSlot)
}

func (u *pgreplSymUnion) startReplication() *pgrepltree.StartReplication {
  return u.val.(*pgrepltree.StartReplication)
}

func (u *pgreplSymUnion) timelineHistory() *pgrepltree.TimelineHistory {
  return u.val.(*pgrepltree.TimelineHistory)
}

func (u *pgreplSymUnion) option() pgrepltree.Option {
  return u.val.(pgrepltree.Option)
}

func (u *pgreplSymUnion) options() pgrepltree.Options {
  return u.val.(pgrepltree.Options)
}

func (u *pgreplSymUnion) numVal() *tree.NumVal {
  return u.val.(*tree.NumVal)
}

func (u *pgreplSymUnion) expr() tree.Expr {
  return u.val.(tree.Expr)
}

func (u *pgreplSymUnion) bool() bool {
  return u.val.(bool)
}

func (u *pgreplSymUnion) lsn() lsn.LSN {
  return u.val.(lsn.LSN)
}
%}

%union {
  id    int32
  pos   int32
  str   string
  union pgreplSymUnion
}

/* Non-keyword tokens */
%token <str> SCONST IDENT
%token <*tree.NumVal> UCONST
%token <lsn.LSN> RECPTR
%token ERROR

/* Keyword tokens. */
%token K_BASE_BACKUP
%token K_IDENTIFY_SYSTEM
%token K_READ_REPLICATION_SLOT
//%token K_SHOW
%token K_START_REPLICATION
%token K_CREATE_REPLICATION_SLOT
%token K_DROP_REPLICATION_SLOT
%token K_TIMELINE_HISTORY
%token K_WAIT
%token K_TIMELINE
%token K_PHYSICAL
%token K_LOGICAL
%token K_SLOT
%token K_RESERVE_WAL
%token K_TEMPORARY
%token K_TWO_PHASE
%token K_EXPORT_SNAPSHOT
%token K_NOEXPORT_SNAPSHOT
%token K_USE_SNAPSHOT

%type <pgrepltree.ReplicationStatement>  cmd command
%type <pgrepltree.ReplicationStatement>  base_backup start_replication start_logical_replication
        create_replication_slot drop_replication_slot identify_system
        read_replication_slot timeline_history
        // show
%type <pgrepltree.Options>  generic_option_list
%type <pgrepltree.Option>  generic_option
%type <*tree.NumVal>  opt_timeline
%type <pgrepltree.Options>  plugin_options plugin_opt_list
%type <pgrepltree.Option>  plugin_opt_elem
%type <tree.Expr>  plugin_opt_arg
%type <str>    opt_slot ident_or_keyword
%type <bool>  opt_temporary
%type <pgrepltree.Options>  create_slot_options create_slot_legacy_opt_list
%type <pgrepltree.Option>  create_slot_legacy_opt

%%

cmd: command opt_semicolon
        {
          pgrepllex.(*lexer).stmt = $1.replicationStatement()
        }
      ;

opt_semicolon:  ';'
        | /* EMPTY */
        ;

command:
      identify_system
      | base_backup
      | start_replication
      | start_logical_replication
      | create_replication_slot
      | drop_replication_slot
      | read_replication_slot
      | timeline_history
      ;

/*
 * IDENTIFY_SYSTEM
 */
identify_system:
      K_IDENTIFY_SYSTEM
        {
          $$.val = &pgrepltree.IdentifySystem{}
        }
      ;

/*
 * READ_REPLICATION_SLOT %s
 */
read_replication_slot:
      /* NOTE(otan): pg has var_name instead of IDENT, but is unclear why */
      K_READ_REPLICATION_SLOT IDENT
        {
          $$.val = &pgrepltree.ReadReplicationSlot{
            Slot: tree.Name($2),
          }
        }
      ;

/*
 * SHOW setting
 * NOTE(otan): we omit K_SHOW as the fallback from being parsed by
 * the replication protocol is to use the normal SQL protocol.
 * The normal SQL protocol already handles SHOW.
show:
      K_SHOW var_name
        {
          VariableShowStmt *n = makeNode(VariableShowStmt);
          n->name = $2;
          $$ = (Node *) n;
        }

var_name:  IDENT  { $$ = $1; }
      | var_name '.' IDENT
        { $$ = fmt.Sprintf("%s.%s", $1, $3); }
    ;
*/

/*
 * BASE_BACKUP [ ( option [ 'value' ] [, ...] ) ]
 */
base_backup:
      K_BASE_BACKUP '(' generic_option_list ')'
        {
          $$.val = &pgrepltree.BaseBackup{
            Options: $3.options(),
          }
        }
      | K_BASE_BACKUP
        {
          $$.val = &pgrepltree.BaseBackup{}
        }
      ;

create_replication_slot:
      /* CREATE_REPLICATION_SLOT slot [TEMPORARY] PHYSICAL [options] */
      K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_PHYSICAL create_slot_options
        {
          $$.val = &pgrepltree.CreateReplicationSlot{
            Slot: tree.Name($2),
            Temporary: $3.bool(),
            Kind: pgrepltree.PhysicalReplication,
            Options: $5.options(),
          }
        }
      /* CREATE_REPLICATION_SLOT slot [TEMPORARY] LOGICAL plugin [options] */
      | K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_LOGICAL IDENT create_slot_options
        {
          $$.val = &pgrepltree.CreateReplicationSlot{
            Slot: tree.Name($2),
            Temporary: $3.bool(),
            Kind: pgrepltree.LogicalReplication,
            Plugin: tree.Name($5),
            Options: $6.options(),
          }
        }
      ;

create_slot_options:
      '(' generic_option_list ')'      { $$ = $2; }
      | create_slot_legacy_opt_list    { $$ = $1; }
      ;

create_slot_legacy_opt_list:
      create_slot_legacy_opt_list create_slot_legacy_opt
        { $$.val = append($1.options(), $2.option()) }
      | /* EMPTY */
        { $$.val = pgrepltree.Options(nil); }
      ;

create_slot_legacy_opt:
      K_EXPORT_SNAPSHOT
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name("snapshot"),
            Value: tree.NewStrVal("export"),
          }
        }
      | K_NOEXPORT_SNAPSHOT
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name("snapshot"),
            Value: tree.NewStrVal("nothing"),
          }
        }
      | K_USE_SNAPSHOT
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name("snapshot"),
            Value: tree.NewStrVal("use"),
          }
        }
      | K_RESERVE_WAL
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name("reserve_wal"),
            Value: tree.NewStrVal("true"),
          }
        }
      | K_TWO_PHASE
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name("two_phase"),
            Value: tree.NewStrVal("true"),
          }
        }
      ;

/* DROP_REPLICATION_SLOT slot */
drop_replication_slot:
      K_DROP_REPLICATION_SLOT IDENT
        {
          $$.val = &pgrepltree.DropReplicationSlot{
            Slot: tree.Name($2),
          }
        }
      | K_DROP_REPLICATION_SLOT IDENT K_WAIT
        {
          $$.val = &pgrepltree.DropReplicationSlot{
            Slot: tree.Name($2),
            Wait: true,
          }
        }
      ;

/*
 * START_REPLICATION [SLOT slot] [PHYSICAL] %X/%X [TIMELINE %d]
 */
start_replication:
      K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline
        {
          ret := &pgrepltree.StartReplication{
            Slot: tree.Name($2),
            Kind: pgrepltree.PhysicalReplication,
            LSN: $4.lsn(),
          }
          if $5.val != nil {
            ret.Timeline = $5.numVal()
          }
          $$.val = ret
        }
      ;

/* START_REPLICATION SLOT slot LOGICAL %X/%X options */
start_logical_replication:
      K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options
        {
          $$.val = &pgrepltree.StartReplication{
            Slot: tree.Name($3),
            Kind: pgrepltree.LogicalReplication,
            LSN: $5.lsn(),
            Options: $6.options(),
          }
        }
      ;
/*
 * TIMELINE_HISTORY %d
 */
timeline_history:
      K_TIMELINE_HISTORY UCONST
        {
          if i, err := $2.numVal().AsInt64(); err != nil || uint64(i) <= 0 {
              pgrepllex.(*lexer).setErr(pgerror.Newf(pgcode.Syntax, "expected a positive integer for timeline"))
              return 1
          }
          $$.val = &pgrepltree.TimelineHistory{Timeline: $2.numVal()}
        }
      ;

opt_physical:
      K_PHYSICAL
      | /* EMPTY */
      ;

opt_temporary:
      K_TEMPORARY            { $$.val = true; }
      | /* EMPTY */          { $$.val = false; }
      ;

opt_slot:
      K_SLOT IDENT
        { $$ = $2; }
      | /* EMPTY */
        { $$ = ""; }
      ;

opt_timeline:
      K_TIMELINE UCONST
        {
          if i, err := $2.numVal().AsInt64(); err != nil || uint64(i) <= 0 {
              pgrepllex.(*lexer).setErr(pgerror.Newf(pgcode.Syntax, "expected a positive integer for timeline"))
              return 1
          }
          $$.val = $2.numVal();
        }
        | /* EMPTY */      { $$.val = nil; }
      ;


plugin_options:
      '(' plugin_opt_list ')'      { $$ = $2; }
      | /* EMPTY */          { $$.val = pgrepltree.Options(nil); }
    ;

plugin_opt_list:
      plugin_opt_elem
        {
          $$.val = pgrepltree.Options{$1.option()}
        }
      | plugin_opt_list ',' plugin_opt_elem
        {
          $$.val = append($1.options(), $3.option())
        }
    ;

plugin_opt_elem:
      IDENT plugin_opt_arg
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name($1),
            Value: $2.expr(),
          }
        }
    ;

plugin_opt_arg:
      SCONST              { $$.val = tree.NewStrVal($1); }
      | /* EMPTY */          { $$.val = tree.Expr(nil); }
    ;

generic_option_list:
      generic_option_list ',' generic_option
        { $$.val = append($1.options(), $3.option()); }
      | generic_option
        { $$.val = pgrepltree.Options{$1.option()} }
      ;

generic_option:
      ident_or_keyword
        {
          $$.val = pgrepltree.Option{Key: tree.Name($1)}
        }
      | ident_or_keyword IDENT
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name($1),
            Value: tree.NewStrVal($2),
          }
        }
      | ident_or_keyword SCONST
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name($1),
            Value: tree.NewStrVal($2),
          }
        }
      | ident_or_keyword UCONST
        {
          $$.val = pgrepltree.Option{
            Key: tree.Name($1),
            Value: $2.numVal(),
          }
        }
      ;

ident_or_keyword:
      IDENT              { $$ = $1; }
      | K_BASE_BACKUP          { $$ = "base_backup"; }
      | K_IDENTIFY_SYSTEM        { $$ = "identify_system"; }
      //| K_SHOW            { $$ = "show"; }
      | K_START_REPLICATION      { $$ = "start_replication"; }
      | K_CREATE_REPLICATION_SLOT  { $$ = "create_replication_slot"; }
      | K_DROP_REPLICATION_SLOT    { $$ = "drop_replication_slot"; }
      | K_TIMELINE_HISTORY      { $$ = "timeline_history"; }
      | K_WAIT            { $$ = "wait"; }
      | K_TIMELINE          { $$ = "timeline"; }
      | K_PHYSICAL          { $$ = "physical"; }
      | K_LOGICAL            { $$ = "logical"; }
      | K_SLOT            { $$ = "slot"; }
      | K_RESERVE_WAL          { $$ = "reserve_wal"; }
      | K_TEMPORARY          { $$ = "temporary"; }
      | K_TWO_PHASE          { $$ = "two_phase"; }
      | K_EXPORT_SNAPSHOT        { $$ = "export_snapshot"; }
      | K_NOEXPORT_SNAPSHOT      { $$ = "noexport_snapshot"; }
      | K_USE_SNAPSHOT        { $$ = "use_snapshot"; }
    ;

%%
