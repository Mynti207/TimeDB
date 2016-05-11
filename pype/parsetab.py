
# parsetab.py
# This file is automatically generated. Do not edit.
_tabversion = '3.8'

_lr_method = 'LALR'

_lr_signature = '674B03BF23F0780B988AC9F0B7D3EDAD'
    
_lr_action_items = {'OP_DIV':([13,],[22,]),'OP_SUB':([13,],[19,]),'RPAREN':([11,15,16,17,24,25,26,29,30,31,33,34,35,36,37,38,40,41,42,43,44,45,46,47,48,49,50,51,52,55,56,57,58,],[18,-28,-27,-26,35,37,42,-30,45,46,48,49,-21,50,-13,51,-17,-15,-11,55,-29,-23,-24,56,-25,-22,-20,-12,-14,-10,-19,58,-16,]),'LBRACE':([0,1,2,3,7,8,18,28,],[6,-5,-4,6,-2,-3,-6,-7,]),'LPAREN':([0,1,2,3,7,8,10,12,14,15,16,17,18,19,20,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,40,41,42,43,44,45,46,48,49,50,51,52,55,56,58,],[4,-5,-4,4,-2,-3,13,-9,13,-28,-27,-26,-6,13,13,13,13,13,39,39,-8,-7,-30,13,13,13,13,13,-21,13,-13,39,-17,-15,-11,39,-29,-23,-24,-25,-22,-20,-12,-14,-10,-19,-16,]),'RBRACE':([12,14,15,16,17,27,35,37,42,45,46,48,49,50,51,55,56,],[-9,28,-28,-27,-26,-8,-21,-13,-11,-23,-24,-25,-22,-20,-12,-10,-19,]),'$end':([1,2,3,5,7,8,18,28,],[-5,-4,-1,0,-2,-3,-6,-7,]),'OUTPUT':([13,],[25,]),'STRING':([10,12,14,15,16,17,19,20,22,23,24,27,29,30,31,32,33,34,35,36,37,42,44,45,46,48,49,50,51,55,56,],[15,-9,15,-28,-27,-26,15,15,15,15,15,-8,-30,15,15,15,15,15,-21,15,-13,-11,-29,-23,-24,-25,-22,-20,-12,-10,-19,]),'ID':([6,9,10,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,48,49,50,51,52,53,54,55,56,58,],[10,11,17,-9,24,17,-28,-27,-26,17,17,32,17,17,17,40,40,-8,-30,17,17,17,17,17,-21,17,-13,40,54,-17,-15,-11,40,-29,-23,-24,-25,-22,-20,-12,-14,57,-18,-10,-19,-16,]),'OP_ADD':([13,],[23,]),'NUMBER':([10,12,14,15,16,17,19,20,22,23,24,27,29,30,31,32,33,34,35,36,37,42,44,45,46,48,49,50,51,55,56,],[16,-9,16,-28,-27,-26,16,16,16,16,16,-8,-30,16,16,16,16,16,-21,16,-13,-11,-29,-23,-24,-25,-22,-20,-12,-10,-19,]),'OP_MUL':([13,],[20,]),'INPUT':([13,],[26,]),'ASSIGN':([13,],[21,]),'IMPORT':([4,],[9,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression':([10,14,19,20,22,23,24,30,31,32,33,34,36,],[12,27,29,29,29,29,29,44,44,47,44,44,44,]),'type':([39,],[53,]),'component':([0,3,],[1,7,]),'declaration':([25,26,38,43,],[41,41,52,52,]),'import_statement':([0,3,],[2,8,]),'declaration_list':([25,26,],[38,43,]),'statement_list':([0,],[3,]),'program':([0,],[5,]),'parameter_list':([19,20,22,23,24,],[30,31,33,34,36,]),'expression_list':([10,],[14,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> program","S'",1,None,None,None),
  ('program -> statement_list','program',1,'p_program','parser.py',20),
  ('statement_list -> statement_list component','statement_list',2,'p_statement_list','parser.py',26),
  ('statement_list -> statement_list import_statement','statement_list',2,'p_statement_list','parser.py',27),
  ('statement_list -> import_statement','statement_list',1,'p_statement_list','parser.py',28),
  ('statement_list -> component','statement_list',1,'p_statement_list','parser.py',29),
  ('import_statement -> LPAREN IMPORT ID RPAREN','import_statement',4,'p_import_statement','parser.py',38),
  ('component -> LBRACE ID expression_list RBRACE','component',4,'p_component','parser.py',43),
  ('expression_list -> expression_list expression','expression_list',2,'p_expression_list','parser.py',49),
  ('expression_list -> expression','expression_list',1,'p_expression_list','parser.py',50),
  ('expression -> LPAREN INPUT declaration_list RPAREN','expression',4,'p_inputexpression','parser.py',59),
  ('expression -> LPAREN INPUT RPAREN','expression',3,'p_inputexpression','parser.py',60),
  ('expression -> LPAREN OUTPUT declaration_list RPAREN','expression',4,'p_outputexpression','parser.py',68),
  ('expression -> LPAREN OUTPUT RPAREN','expression',3,'p_outputexpression','parser.py',69),
  ('declaration_list -> declaration_list declaration','declaration_list',2,'p_declaration_list','parser.py',77),
  ('declaration_list -> declaration','declaration_list',1,'p_declaration_list','parser.py',78),
  ('declaration -> LPAREN type ID RPAREN','declaration',4,'p_declaration','parser.py',87),
  ('declaration -> ID','declaration',1,'p_declaration','parser.py',88),
  ('type -> ID','type',1,'p_type','parser.py',97),
  ('expression -> LPAREN ASSIGN ID expression RPAREN','expression',5,'p_expression_assign','parser.py',103),
  ('expression -> LPAREN ID parameter_list RPAREN','expression',4,'p_expression_id_parens','parser.py',108),
  ('expression -> LPAREN ID RPAREN','expression',3,'p_expression_id_parens','parser.py',109),
  ('expression -> LPAREN OP_ADD parameter_list RPAREN','expression',4,'p_op_add_expression','parser.py',118),
  ('expression -> LPAREN OP_SUB parameter_list RPAREN','expression',4,'p_op_sub_expression','parser.py',125),
  ('expression -> LPAREN OP_MUL parameter_list RPAREN','expression',4,'p_op_mul_expression','parser.py',132),
  ('expression -> LPAREN OP_DIV parameter_list RPAREN','expression',4,'p_op_div_expression','parser.py',139),
  ('expression -> ID','expression',1,'p_expression_id','parser.py',145),
  ('expression -> NUMBER','expression',1,'p_expression_number','parser.py',150),
  ('expression -> STRING','expression',1,'p_expression_string','parser.py',155),
  ('parameter_list -> parameter_list expression','parameter_list',2,'p_parameter_list','parser.py',160),
  ('parameter_list -> expression','parameter_list',1,'p_parameter_list','parser.py',161),
]
