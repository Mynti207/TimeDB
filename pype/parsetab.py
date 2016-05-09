
# parsetab.py
# This file is automatically generated. Do not edit.
_tabversion = '3.8'

_lr_method = 'LALR'

_lr_signature = '6A65CD8BFBEB79DCE9C7C577BDAF4664'
    
_lr_action_items = {'$end':([3,4,5,6,9,10,18,20,],[0,-5,-4,-1,-2,-3,-6,-7,]),'NUMBER':([8,12,13,14,15,17,19,23,24,25,26,28,29,33,35,36,37,38,39,41,42,43,48,49,50,51,52,53,54,55,56,],[12,-27,-9,12,-28,-26,-8,12,12,12,12,12,12,-13,-30,12,12,12,12,-11,12,-21,-12,-29,-25,-22,-24,-23,-10,-20,-19,]),'OP_ADD':([16,],[24,]),'ASSIGN':([16,],[21,]),'IMPORT':([1,],[7,]),'RPAREN':([11,12,15,17,22,27,28,31,32,33,34,35,36,37,38,39,40,41,42,43,44,47,48,49,50,51,52,53,54,55,56,57,58,],[18,-27,-28,-26,33,41,43,-15,48,-13,-17,-30,50,51,52,53,54,-11,55,-21,56,-14,-12,-29,-25,-22,-24,-23,-10,-20,-19,58,-16,]),'STRING':([8,12,13,14,15,17,19,23,24,25,26,28,29,33,35,36,37,38,39,41,42,43,48,49,50,51,52,53,54,55,56,],[15,-27,-9,15,-28,-26,-8,15,15,15,15,15,15,-13,-30,15,15,15,15,-11,15,-21,-12,-29,-25,-22,-24,-23,-10,-20,-19,]),'LPAREN':([0,4,5,6,8,9,10,12,13,14,15,17,18,19,20,22,23,24,25,26,27,28,29,31,32,33,34,35,36,37,38,39,40,41,42,43,47,48,49,50,51,52,53,54,55,56,58,],[1,-5,-4,1,16,-2,-3,-27,-9,16,-28,-26,-6,-8,-7,30,16,16,16,16,30,16,16,-15,30,-13,-17,-30,16,16,16,16,30,-11,16,-21,-14,-12,-29,-25,-22,-24,-23,-10,-20,-19,-16,]),'RBRACE':([12,13,14,15,17,19,33,41,43,48,50,51,52,53,54,55,56,],[-27,-9,20,-28,-26,-8,-13,-11,-21,-12,-25,-22,-24,-23,-10,-20,-19,]),'OP_MUL':([16,],[25,]),'OP_SUB':([16,],[26,]),'LBRACE':([0,4,5,6,9,10,18,20,],[2,-5,-4,2,-2,-3,-6,-7,]),'OP_DIV':([16,],[23,]),'OUTPUT':([16,],[22,]),'INPUT':([16,],[27,]),'ID':([2,7,8,12,13,14,15,16,17,19,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,45,46,47,48,49,50,51,52,53,54,55,56,58,],[8,11,17,-27,-9,17,-28,28,-26,-8,29,34,17,17,17,17,34,17,17,46,-15,34,-13,-17,-30,17,17,17,17,34,-11,17,-21,57,-18,-14,-12,-29,-25,-22,-24,-23,-10,-20,-19,-16,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'declaration':([22,27,32,40,],[31,31,47,47,]),'program':([0,],[3,]),'type':([30,],[45,]),'component':([0,6,],[4,9,]),'expression_list':([8,],[14,]),'declaration_list':([22,27,],[32,40,]),'import_statement':([0,6,],[5,10,]),'parameter_list':([23,24,25,26,28,],[36,37,38,39,42,]),'expression':([8,14,23,24,25,26,28,29,36,37,38,39,42,],[13,19,35,35,35,35,35,44,49,49,49,49,49,]),'statement_list':([0,],[6,]),}

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
