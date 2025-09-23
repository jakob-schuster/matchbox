use parser::{prog, tm};

use crate::{
    surface::*,
    util::{self, Location},
    GlobalConfig,
};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct ParseError {
    pub location: Location,
    pub message: String,
}

pub fn parse(string: &str, global_config: &GlobalConfig) -> Result<Prog, ParseError> {
    prog(string, global_config).map_err(|e| ParseError {
        location: Location::new(e.location.offset, e.location.offset + 1),
        message: "Parse error".to_string(),
    })
}

pub fn parse_tm(string: &str, global_config: &GlobalConfig) -> Result<Tm, ParseError> {
    tm(string, global_config).map_err(|e| ParseError {
        location: Location::new(e.location.offset, e.location.offset + 1),
        message: "Parse error".to_string(),
    })
}

peg::parser! {
    grammar parser(global_config: &GlobalConfig) for str {

        pub rule prog() -> Prog = located(<prog_data()>);

        pub rule prog_data() -> ProgData
            = _ stmts:whitespace_sensitive_list(<stmt()>, <whitespace_except_newline() [';'|'\n']() _>) { ProgData { stmts } }

        //

        rule stmt() -> Stmt = stmt:located(<stmt_data()>) { stmt }
        rule stmt_data() -> StmtData
            = name:name() _ "=" _ tm:tm() { StmtData::Let { name, tm } }
            // for now, just keep it to two kinds of if statement - boolean conditionals and match statements
            / "if" _ tm:tm() _ "matches" _ branch:pattern_branch() { StmtData::If { branches: vec![Branch::new(tm.location.clone(), BranchData::Is { tm, branches: vec![branch] })] } }
            / "if" _ tm:tm() _ "matches" _ "{" _ branches:whitespace_sensitive_list_without_final_delimiter(<pattern_branch()>, <whitespace_except_newline() [','|'\n']() _>) _ "}" { StmtData::If { branches: vec![Branch::new(tm.location.clone(), BranchData::Is { tm, branches })] } }
            / "if"  _ tm:tm() _ stmt1:group_stmt() _ "else" _ stmt2:group_stmt() { StmtData::If { branches: vec![Branch { location: tm.location.clone(), data: BranchData::Bool { tm: tm.clone(), stmt: stmt1 } }, Branch { location: tm.location.clone(), data: BranchData::Bool { tm: Tm { location: tm.location.clone(), data: TmData::BoolLit { b: true } }, stmt: stmt2 } }] } }
            / "if"  _ tm:tm() _ stmt:group_stmt() { StmtData::If { branches: vec![Branch { location: tm.location.clone(), data: BranchData::Bool { tm, stmt } }] } }
            / tm:tm() { StmtData::Tm { tm } }

        rule group_stmt() -> Stmt = stmt:located(<group_stmt_data()>) { stmt }
        rule group_stmt_data() -> StmtData
            = "{" _ stmts:whitespace_sensitive_list(<stmt()>, <whitespace_except_newline() [';'|'\n']() _>) _ "}" { StmtData::Group { stmts } }
            // single statements don't need braces
            / stmt_data()

        //

        rule branch() -> Branch = branch:located(<branch_data()>) { branch }
        #[cache_left_rec]
        rule branch_data() -> BranchData
            = tm:tm() _ "=>" _ stmt:group_stmt() { BranchData::Bool { tm, stmt } }
            // / tm:tm() _ "is" _ branches:whitespace_sensitive_list_without_final_delimiter(<pattern_branch()>, <whitespace_except_newline() [','|'\n']() _>) { BranchData::Is { tm, branches } }

        //

        rule pattern_branch() -> PatternBranch = located(<pattern_branch_data()>)
        rule pattern_branch_data() -> PatternBranchData
            = pat:pattern() _ "=>" _ stmt:group_stmt() { PatternBranchData { pat, stmt } }

        rule pattern() -> Pattern = located(<pattern_data()>)
        rule pattern_data() -> PatternData
            = name:name() ":" pattern:pattern1() { PatternData::Named { name, pattern: Rc::new(pattern) } }
            / pattern_data1()

        rule pattern1() -> Pattern = located(<pattern_data1()>)
        rule pattern_data1() -> PatternData
            = "_" { PatternData::Hole }
            / b:bool_val() { PatternData::BoolLit { b } }
            / n:num_val() { PatternData::NumLit { n } }
            / "{" _ fields:whitespace_sensitive_list(<rec_pattern_field()>,<whitespace_except_newline() [','|'\n']() _>) _ "}" { PatternData::RecLit { fields } }
            / "[" _ regs:list(<region()>, <_()>) _ "]" _ binds:read_parameters() { PatternData::Read { regs, binds, mode: global_config.match_mode.clone() } }

        //

        rule region() -> Region = located(<region_data()>)
        rule region_data() -> RegionData
            // named region
            = name:name() ":" regs:region_or_group() { RegionData::Named { name, regs } }
            // sized region
            / "|" tm:tm() ":" regs:region_or_group() "|" { RegionData::Sized { tm, regs } }
            / region_data1()

        rule region_or_group() -> Vec<Region>
            = "(" _ regs:list(<region()>, <_()>) _ ")" { regs }
            / reg:region1() { vec![reg] }
        rule region1() -> Region = located(<region_data1()>)
        rule region_data1() -> RegionData
            // hole
            = "_" { RegionData::Hole }
            // sized hole sugar
            / "|" tm:tm() "|" {RegionData::Sized { tm: tm.clone(), regs: vec![Region::new(tm.location.clone(), RegionData::Hole)] } }
            // terms
            /  tm:tm() _ "~" _ error:tm() { RegionData::Term { tm, error } }
            // terms
            / tm:tm() { RegionData::Term { tm, error: Tm::new(Location::new(0,0), TmData::NumLit { n: Num::Float(global_config.error) }) } }

        rule read_parameters() -> Vec<ReadParameter>
            = "for" _ parameters:nonempty_list(<read_parameter()>, <",">) { parameters }
            / "" { vec![] }

        rule read_parameter() -> ReadParameter = located(<read_parameter_data()>)
        rule read_parameter_data() -> ReadParameterData
            = name:name() _ "in" _ tm:tm() { ReadParameterData { name, tm } }
        //

        pub rule tm() -> Tm = located(<tm_data()>)
        #[cache_left_rec]
        rule tm_data() -> TmData
            = arg1:tm() _ "|>" _ head:tm9() "(" _ args_opts:arg_list() _ ")" { TmData::FunApp { head: Rc::new(head), args: [arg1].into_iter().chain(args_opts.0.into_iter()).collect::<Vec<_>>(), opts: args_opts.1 } }
            / tm_data1()

        rule tm1() -> Tm = located(<tm_data1()>)
        #[cache_left_rec]
        rule tm_data1() -> TmData
            = tm0:tm1() _ "or" _ tm1:tm2() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Or } }
            / tm_data2()

        rule tm2() -> Tm = located(<tm_data2()>)
        #[cache_left_rec]
        rule tm_data2() -> TmData
            = tm0:tm2() _ "and" _ tm1:tm3() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::And } }
            / tm_data3()

        rule tm3() -> Tm = located(<tm_data3()>)
        #[cache_left_rec]
        rule tm_data3() -> TmData
            = tm0:tm3() _ "==" _ tm1:tm4() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Equal } }
            / tm0:tm3() _ "!=" _ tm1:tm4() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::NotEqual } }
            / tm_data4()

        rule tm4() -> Tm = located(<tm_data4()>)
        #[cache_left_rec]
        rule tm_data4() -> TmData
            = tm0:tm4() _ "<" _ tm1:tm5() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::LessThan } }
            / tm0:tm4() _ ">" _ tm1:tm5() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::GreaterThan } }
            / tm0:tm4() _ "<=" _ tm1:tm5() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::LessThanOrEqual } }
            / tm0:tm4() _ ">=" _ tm1:tm5() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::GreaterThanOrEqual } }
            / tm_data5()

        rule tm5() -> Tm = located(<tm_data5()>)
        #[cache_left_rec]
        rule tm_data5() -> TmData
            = tm0:tm5() _ "+" _ tm1:tm6() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Plus } }
            / tm0:tm5() _ "-" _ tm1:tm6() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Minus } }
            / tm_data6()

        rule tm6() -> Tm = located(<tm_data6()>)
        #[cache_left_rec]
        rule tm_data6() -> TmData
            // tighter binary operations
            = tm0:tm6() _ "*" _ tm1:tm7() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Times } }
            / tm0:tm6() _ "/" _ tm1:tm7() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Division } }
            / tm0:tm6() _ "%" _ tm1:tm7() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Modulo } }
            / tm0:tm6() _ "^" _ tm1:tm7() { TmData::BinOp { tm0: Rc::new(tm0), tm1: Rc::new(tm1), op: BinOp::Exponent } }
            / tm_data7()

        rule tm7() -> Tm = located(<tm_data7()>)
        #[cache_left_rec]
        rule tm_data7() -> TmData
            // unary operations
            = "-" tm:tm8() { TmData::UnOp { tm: Rc::new(tm), op: UnOp::Minus } }
            / "not" _ tm:tm8() { TmData::UnOp { tm: Rc::new(tm), op: UnOp::Not } }
            / tm_data8()

        rule tm8() -> Tm = located(<tm_data8()>)
        #[cache_left_rec]
        rule tm_data8() -> TmData
            // these three all need to be on the same precedence level - not sure why. investigate more later

            // method application
            = arg1:tm8() _ "." head:tm9() "(" _ args_opts:arg_list() _ ")" { TmData::FunApp { head: Rc::new(head), args: [arg1].into_iter().chain(args_opts.0.into_iter()).collect::<Vec<_>>(), opts: args_opts.1 } }
            // record projection
            / tm:tm8() _ "." name:name() { TmData::RecProj { tm: Rc::new(tm), name } }
            // function application
            / head:tm8() "(" _ args_opts:arg_list()_ ")" { TmData::FunApp { head: Rc::new(head), args: args_opts.0, opts: args_opts.1 } }
            / tm_data9()

        rule tm9() -> Tm = located(<tm_data9()>)
        #[cache_left_rec]
        rule tm_data9() -> TmData
            = tm_data10()

        rule tm10() -> Tm = located(<tm_data10()>)
        #[cache_left_rec]
        rule tm_data10() -> TmData
            // boolean literals
            = b:bool_val() { TmData::BoolLit { b } }
            // numeric literals
            / n:num_val() { TmData::NumLit { n } }
            // string literals
            / "'" regs:whitespace_sensitive_list(<str_lit_region()>, <""()>) "'" { TmData::StrLit { regs } }
            // nucleotide sequence literals
            / s:located(<['G'|'T'|'A'|'C']+>) &[c if !c.is_alphanumeric() && c != '_'] { TmData::StrLit { regs: vec![StrLitRegion::new(s.location, StrLitRegionData::Str { s: s.data.into_iter().map(|c| c as u8).collect() })] } }

            // record literals
            // WARN currently the empty record {} and the empty record type {} are indistinguishable.
            // preference the empty record (because who is even writing empty record types?)
            / "{" _ fields:whitespace_sensitive_list(<rec_lit_field()>,<whitespace_except_newline() [','|'\n']() _>) _ "}" { TmData::RecLit { fields }}
            // record type literals
            / "{" _ fields:whitespace_sensitive_list(<rec_ty_field()>,<whitespace_except_newline() [','|'\n']() _>) _ "}" { TmData::RecTy { fields }}
            / "{" _ fields:whitespace_sensitive_list(<rec_ty_field()>,<whitespace_except_newline() [','|'\n']() _>) _ ".." _ "}" { TmData::RecWithTy { fields }}

            // list type literals (with the clunky $ to separate list type literals from singleton lists)
            / "$[" _ tm:tm() _ "]" { TmData::ListTy { tm: Rc::new(tm) } }
            // list literals
            / "[" _ tms:whitespace_sensitive_list(<tm()>,<whitespace_except_newline() [','|'\n']() _>) _ "]" { TmData::ListLit { tms } }

            // function type literals
            / "(" _ args_opts:list_then(<tm()>, <opt_param()>, <",">) _ ")" _ "->" _ body:tm() { TmData::FunTy { args: args_opts.0, body: Rc::new(body), opts: args_opts.1 } }
            // function literals
            / "(" _ args_opts:param_list() _ ")" _ "=>" _ body:tm() { TmData::FunLit { args: args_opts.0, opts: args_opts.1, body: Rc::new(body) }}
            // foreign function literals
            / "$(" _ args_opts:param_list() _ ")" _ ":" _ ty:tm() _ "=>" _ name:name() { TmData::FunLitForeign { args: args_opts.0, opts: args_opts.1, ty: Rc::new(ty), name } }

            // named things
            / name:name() { TmData::Name { name } }

            // grouping
            / "(" _ tm_data:tm_data() _ ")" { tm_data }

        //

        rule param() -> Param
            = name:name() _ ":" _ ty:tm() { Param { name, ty } }
        rule opt_param() -> OptParam
            = name:name() _ ":" _ ty:tm() _ "=" _ tm:tm() { OptParam { name, ty, tm } }
        rule opt_arg() -> (String, Tm)
            = name:name() _ "=" _ tm:tm() { (name, tm) }

        // WARN this feels like a stupid way to parse parameter lists; revise this
        rule param_list() -> (Vec<Param>, Vec<OptParam>)
            = rest:opt_param_list() { (vec![], rest) }
            / p:param() _ "," _ rest:param_list() { (vec![p].into_iter().chain(rest.0).collect(), rest.1) }
            / p:param() { (vec![p], vec![]) }
            / "" { (vec![], vec![]) }
        rule opt_param_list() -> Vec<OptParam>
            = o:opt_param() _ "," _ rest:opt_param_list() { vec![o].into_iter().chain(rest).collect() }
            / o:opt_param() (_ ",")? { vec![o] }

        rule arg_list() -> (Vec<Tm>, Vec<(String, Tm)>)
            = rest:opt_arg_list() { (vec![], rest) }
            / p:tm() _ "," _ rest:arg_list() { (vec![p].into_iter().chain(rest.0).collect(), rest.1) }
            / p:tm() { (vec![p], vec![]) }
            / "" { (vec![], vec![]) }
        rule opt_arg_list() -> Vec<(String, Tm)>
            = o:opt_arg() _ "," _ rest:opt_arg_list() { vec![o].into_iter().chain(rest).collect() }
            / o:opt_arg() (_ ",")? { vec![o] }

        rule rec_lit_field() -> util::RecField<Tm>
            = name:name() _ "=" _ tm:tm() { util::RecField::new(name, tm) }

        rule rec_ty_field() -> util::RecField<Tm>
            = name:name() _ ":" _ tm:tm() { util::RecField::new(name, tm) }

        rule rec_pattern_field() -> util::RecField<Pattern>
            = name:name() _ "=" _ pattern:pattern() { util::RecField::new(name, pattern) }



        rule str_lit_char() -> char
            = "\\n" { '\n' }
            / "\\t" { '\t' }
            / "\\\\" { '\\' }
            / c:[c if c.is_ascii() && c != '\'' && c != '{' && c != '}' && c != '\\' ] { c }

        rule str_lit_region() -> StrLitRegion = located(<str_lit_region_data()>)
        rule str_lit_region_data() -> StrLitRegionData
            = "{" tm:tm() "}" { StrLitRegionData::Tm { tm } }
            / s:str_lit_char()+ { StrLitRegionData::Str { s: s.into_iter().map(|c| c as u8).collect() } }

        //

        rule bool_val() -> bool
            = "true" { true }
            / "false" { false }
        rule num_val() -> Num
            = n:$(['0'..='9']+ "." ['0'..='9']+) { Num::Float(n.parse::<f32>().unwrap()) }
            / n:$(['0'..='9']+) { Num::Int(n.parse::<i32>().unwrap()) }

        rule name() -> String
            = s:$(['a'..='z' | 'A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '_' | '0'..='9' | '!']*) { s.to_string() }

        //

        rule located<T>(tr: rule<T>) -> util::Located<T>
            = start:position!() t:tr() end:position!() { util::Located::new(util::Location::new(start, end), t) }

        // WARN doesn't work
        rule list_then<T,V>(tr: rule<T>, tr2: rule<V>, sepr: rule<()>) -> (Vec<T>, Vec<V>)
            = v:(t:tr() ** (_ sepr() _) {t}) _ sepr() _ v2:list(<tr2()>, <sepr()>)
                { (v, v2) }
            / v:list(<tr()>, <sepr()>)
                { (v, vec![]) }
        rule list<T>(tr: rule<T>, sepr: rule<()>) -> Vec<T>
            = v:(t:tr() ** (_ sepr() _) {t}) (_ sepr() _)?
                { v }
        rule nonempty_list<T>(tr: rule<T>, sepr: rule<()>) -> Vec<T>
            = v:(t:tr() ++ (_ sepr() _) {t}) (_ sepr() _)?
                { v }

        rule whitespace_sensitive_list<T>(tr: rule<T>, sepr: rule<()>) -> Vec<T>
            = v:(t:tr() ** sepr() {t}) sepr()?
                { v }

        rule whitespace_sensitive_list_without_final_delimiter<T>(tr: rule<T>, sepr: rule<()>) -> Vec<T>
            = v:(t:tr() ** sepr() {t})
                { v }

        //

        rule whitespace() = quiet!{[' ' | '\n' | '\t']*}
        rule whitespace_except_newline() = quiet!{ [' ' | '\t']* }
        rule _
            = whitespace() "#" [c if c != '\n']* "\n" _()
            / whitespace()
    }
}
