%option reentrant bison-bridge
%option noyywrap nounput noinput
%option noyyalloc
%option noyyrealloc
%option noyyfree

%{
#include "veclang_parser.h"
#define RETURN(TOKEN) return yyextra = TOKEN
#define NODE(TOKEN) *yylval = veclang_new_value_node(yyscanner, VLN_Str, yytext); RETURN(TOKEN)
static int can_end_line(int n) {
  return n == NUM || n == ID || n == ')';
}
%}

%%
"QUERY" NODE(QUERY)
"FUNCTION" NODE(FUNCTION);
"WHILE" NODE(WHILE);
"EMIT" NODE(EMIT);

[0-9]*							*yylval = veclang_new_value_node(yyscanner, VLN_Int, yytext); RETURN(NUM);
[[:alpha:]_][[:alnum:]_]*		NODE(ID);

#.*								// Comment
[ \t\r]*						// Whitespace

\n 								if (can_end_line(yyextra)) RETURN(';');
.								RETURN(yytext[0]);
<<EOF>>							if (can_end_line(yyextra)) RETURN(';'); else yyterminate();

%%
