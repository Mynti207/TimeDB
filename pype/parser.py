import ply.yacc

from pype.lexer import tokens, reserved
from pype.ast import *

'''
Production rules for all grammar rules; built in conjunction with Abstract
Syntax Trees (AST).

Includes:
    Production rules
    Error-handling functions

Builds parser
'''


# constructs AST nodes
def p_program(p):
    r'program : statement_list'
    p[0] = ASTProgram(p[1])


# aggregates list of AST nodes
def p_statement_list(p):
    r'''statement_list : statement_list component
                       | statement_list import_statement
                       | import_statement
                       | component'''

    if len(p) > 2:
        p[0] = p[1] + [p[2]]
    else:
        p[0] = [p[1]]


def p_import_statement(p):
    r'import_statement : LPAREN IMPORT ID RPAREN'
    p[0] = ASTImport(p[3])


def p_component(p):
    r'''component : LBRACE ID expression_list RBRACE'''
    p[2] = ASTID(p[2])
    p[0] = ASTComponent([p[2]] + p[3])


def p_expression_list(p):
    r'''expression_list : expression_list expression
                        | expression'''
    if len(p) > 2:
        p[1].append(p[2])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_inputexpression(p):
    r'''expression : LPAREN INPUT declaration_list RPAREN
                   | LPAREN INPUT RPAREN'''
    if len(p) > 4:
        p[0] = ASTInputExpr(p[3])
    else:
        p[0] = ASTInputExpr()


def p_outputexpression(p):
    r'''expression : LPAREN OUTPUT declaration_list RPAREN
                   | LPAREN OUTPUT RPAREN'''
    if len(p) > 4:
        p[0] = ASTOutputExpr(p[3])
    else:
        p[0] = ASTOutputExpr()


def p_declaration_list(p):
    r'''declaration_list : declaration_list declaration
                         | declaration'''
    if len(p) > 2:
        p[1].append(p[2])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_declaration(p):
    r'''declaration : LPAREN type ID RPAREN
                    | ID'''
    # Here we ignore the type for now
    if len(p) > 2:
        p[0] = ASTID(p[3])
    else:
        p[0] = ASTID(p[1])


def p_type(p):
    # ignore the type for now
    r'''type : ID'''
    p[0] = ASTID(p[1])


def p_expression_assign(p):
    r'''expression : LPAREN ASSIGN ID expression RPAREN'''
    p[0] = ASTAssignmentExpr((ASTID(p[3]), p[4]))


def p_expression_id_parens(p):
    r'''expression : LPAREN ID parameter_list RPAREN
                   | LPAREN ID RPAREN'''
    if len(p) > 4:
        p[0] = ASTEvalExpr([ASTID(p[2])] + p[3])
    else:
        p[0] = ASTID(p[2])


# def p_expression_plus(p):
def p_op_add_expression(p):
    r'''expression : LPAREN OP_ADD parameter_list RPAREN'''
    # p[0] = ASTEvalExpr([ASTID(p[2])] + p[3])
    p[0] = ASTEvalExpr([ASTID(name='__add__')] + p[3])


# def p_expression_minus(p):
def p_op_sub_expression(p):
    r'''expression : LPAREN OP_SUB parameter_list RPAREN'''
    # p[0] = ASTEvalExpr([ASTID(p[2])] + p[3])
    p[0] = ASTEvalExpr([ASTID(name='__sub__')] + p[3])


# def p_expression_mult(p):
def p_op_mul_expression(p):
    r'''expression : LPAREN OP_MUL parameter_list RPAREN'''
    # p[0] = ASTEvalExpr([ASTID(p[2])] + p[3])
    p[0] = ASTEvalExpr([ASTID(name='__mul__')] + p[3])


# def p_expression_div(p):
def p_op_div_expression(p):
    r'''expression : LPAREN OP_DIV parameter_list RPAREN'''
    # p[0] = ASTEvalExpr([ASTID(p[2])] + p[3])
    p[0] = ASTEvalExpr([ASTID(name='__truediv__')] + p[3])


def p_expression_id(p):
    r'''expression : ID'''
    p[0] = ASTID(p[1])


def p_expression_number(p):
    r'''expression : NUMBER'''
    p[0] = ASTLiteral(p[1])


def p_expression_string(p):
    r'''expression : STRING'''
    p[0] = ASTLiteral(p[1])


def p_parameter_list(p):
    r'''parameter_list : parameter_list expression
                       | expression'''
    if len(p) > 2:
        p[0] = p[1] + [p[2]]
    else:
        p[0] = [p[1]]


def p_error(p):
    if p:
        print("Syntax error at token ", p.type)
        print("Line number ", p.lineno)
        print("Position ", p.lexpos)
        # discard the token and tell the parser it's okay
        parser.errok()
    else:
        print("Syntax error at EOF")

start = 'program'
parser = ply.yacc.yacc()
