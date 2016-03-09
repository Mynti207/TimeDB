from lexer import lexer

# Test it out
data = '''
3 + 4 * 10
% + -20 *2%
'''

print("hey", data.split('\n'))

# Give the lexer some input
lexer.input(data)

# Tokenize
while True:
    tok = lexer.token()
    if not tok:
        break      # No more input
    print(tok)
