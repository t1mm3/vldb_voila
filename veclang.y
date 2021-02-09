%define api.pure full
%lex-param { VecLangScanner *scanner }
%parse-param { VecLangScanner *scanner }

%code requires {
#include "veclang.h"

#define YYSTYPE VecLangNode*
#define YY_EXTRA_TYPE int
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef VecLangScanner *yyscan_t;
#endif
}

%code {
#include "veclang_lexer.h"
int
yyerror(VecLangScanner *scanner, const char *msg) {
	printf("error: %s\n", msg);
	return 0;
}
}

%initial-action { yyset_extra(0, scanner); }

%token NUM ID
%token WHILE FUNCTION EMIT QUERY

%%
main: func { $$ = veclang_new_node1(scanner, VLN_MainList, $1); }
	| main func { $$ = veclang_append_node(scanner, $1, $2); }
	| main query { $$ = veclang_append_node(scanner, $1, $2); }

func: FUNCTION ID '(' ID ')' '{' stmts '}' { $$ = veclang_new_node3(scanner, VLN_Function, $2, $7, $4); }

query: QUERY '{' stmts '}' { $$ = veclang_new_node3(scanner, VLN_Query, $2, $4); }

stmts: // Empty line
	| stmt { $$ = veclang_new_node1(scanner, VLN_StmtList, $1); }
	| stmts stmt { $$ = veclang_append_node(scanner, $1, $2); }

stmt: expr ';' { $$ = veclang_new_node1(scanner, VLN_ExecExpr, $1); }
	| ID '=' expr ';' { $$ = veclang_new_node2(scanner, VLN_Assignment, $1, $3); }
	| WHILE pred '{' stmts '}' { $$ = veclang_new_node2(scanner, VLN_WhileLoop, $4, $2); }
	| EMIT expr ';' { $$ = veclang_new_node1(scanner, VLN_Emit, $2); }

pred: '|' ID { $$ = veclang_new_node1(scanner, VLN_Predicate, $2); }

expr: NUM { $$ = veclang_new_node1(scanner, VLN_ConstantInt, $1); }
	| ID { $$ = veclang_new_node1(scanner, VLN_Identifier, $1); }
	| ID '[' NUM ']' { $$ = veclang_new_node2(scanner, VLN_GetTupleElem, $3, $1); }
	| '(' expr_list ')' { $$ = veclang_new_node1(scanner, VLN_CreateTuple, $2); }
	| expr pred { $$ = veclang_set_predicate($1, $2);}
	| ID '(' expr ')' { $$ = veclang_new_node2(scanner, VLN_Call, $1, $3); }

expr_list: expr { $$ = veclang_new_node1(scanner, VLN_ExprList, $1); }
	| expr_list ',' expr { $$ = veclang_append_node(scanner, $1, $3); }

%%
