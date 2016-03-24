import ply.lex

'''
List of tokens and associated production rules.

Includes:
    Token declarations
    Token specifications
        Reference: PLY documentation, section 4.3
    Additional rules and routines

Builds lexer
'''

# reserved token names
# format: {pattern : token-name}
reserved = {
    'input': 'INPUT',
    'output': 'OUTPUT',
    'import': 'IMPORT',
}

# list of tokens supported by PYPE
# note: 'tokens' is a special word in ply's lexers
tokens = [
    'LPAREN', 'RPAREN',  # individual parentheses
    'LBRACE', 'RBRACE',  # individual braces
    # four basic arithmetic symbols
    'OP_ADD', 'OP_SUB', 'OP_MUL', 'OP_DIV',
    'STRING',  # anything enclosed by double quotes
    'ASSIGN',  # two characters :=
    'NUMBER',  # arbitrary number of digits
    # sequence of letters, numbers, underscores (must not start with number)
    'ID',
] + list(reserved.values())

# LPAREN/RPAREN: individual parenthesis
t_LPAREN = r'\('
t_RPAREN = r'\)'

# LBRACE/RBRACE: individual braces
t_LBRACE = r'\{'
t_RBRACE = r'\}'

# OP_ADD, OP_SUB, OP_MUL, OP_DIV: four basic arithmetic operations
t_OP_ADD = r'\+'
t_OP_SUB = r'\-'
t_OP_MUL = r'\*'
t_OP_DIV = r'\/'


# STRING: anything enclosed by double quotes
def t_STRING(t):
    r'"(.*?)"'
    t.value = str(t.value)
    return t

# ASSIGN: the two characters :=
t_ASSIGN = r'\:='


# NUMBER: an arbitrary number of digits
def t_NUMBER(t):
    r'\d+'
    t.value = int(t.value)
    return t

# WHITESPACE (ignored)
t_ignore = ' \t'


# ID/KEYWORD: sequence of letters, numbers, underscores
# (must not start with number)
# reference: PLY documentation, secton 4.3
def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    t.type = reserved.get(t.value, 'ID')  # check for reserved words
    return t


# COMMENT: denoted by # (same format as Python)
# reference: PLY documentation, section 4.5
def t_COMMENT(t):
    r'\#.*'
    pass  # no return value - token discarded


# tracks line numbers
# reference: PLY documentation, section 4.6
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)


# computes the column position
#     input: input text string
#     token: token instance
def find_column(input, token):
    last_cr = input.rfind('\n', 0, token.lexpos)
    if last_cr < 0:
        last_cr = 0
    column = (token.lexpos - last_cr)
    return column


# error-handling rule: prints line and column numbers
def t_error(t):
    print("Illegal character '%s' at line '%i' col '%i'" % (
        t.value[0], t.lexer.lineno, find_column(t.lexer.lexdata, t)))
    t.lexer.skip(1)

# builds the lexer -->
lexer = ply.lex.lex()
