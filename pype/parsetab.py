
# parsetab.py
# This file is automatically generated. Do not edit.
_tabversion = '3.8'

_lr_method = 'LALR'

_lr_signature = '7B77CFC2E9574BE6AA62E56690011F66'
    
_lr_action_items = {'IMPORT':([2,],[9,]),'$end':([1,3,5,6,7,8,18,19,],[-1,0,-5,-4,-3,-2,-6,-7,]),'RBRACE':([12,13,14,15,17,20,32,35,38,46,48,50,51,52,53,54,57,],[19,-26,-9,-28,-27,-8,-13,-11,-21,-12,-10,-22,-20,-23,-24,-25,-19,]),'OP_ADD':([16,],[23,]),'OUTPUT':([16,],[21,]),'LBRACE':([0,1,5,6,7,8,18,19,],[4,4,-5,-4,-3,-2,-6,-7,]),'NUMBER':([10,12,13,14,15,17,20,23,24,25,26,27,32,35,36,37,38,39,40,41,42,43,46,48,49,50,51,52,53,54,57,],[17,17,-26,-9,-28,-27,-8,17,17,17,17,17,-13,-11,-30,17,-21,17,17,17,17,17,-12,-10,-29,-22,-20,-23,-24,-25,-19,]),'OP_SUB':([16,],[25,]),'STRING':([10,12,13,14,15,17,20,23,24,25,26,27,32,35,36,37,38,39,40,41,42,43,46,48,49,50,51,52,53,54,57,],[15,15,-26,-9,-28,-27,-8,15,15,15,15,15,-13,-11,-30,15,-21,15,15,15,15,15,-12,-10,-29,-22,-20,-23,-24,-25,-19,]),'ID':([4,9,10,12,13,14,15,16,17,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,57,58,],[10,11,13,13,-26,-9,-28,24,-27,-8,31,31,13,13,13,13,13,43,44,31,-17,-13,-15,31,-11,-30,13,-21,13,13,13,13,13,-18,56,-12,-14,-10,-29,-22,-20,-23,-24,-25,-19,-16,]),'INPUT':([16,],[22,]),'LPAREN':([0,1,5,6,7,8,10,12,13,14,15,17,18,19,20,21,22,23,24,25,26,27,30,31,32,33,34,35,36,37,38,39,40,41,42,43,46,47,48,49,50,51,52,53,54,57,58,],[2,2,-5,-4,-3,-2,16,16,-26,-9,-28,-27,-6,-7,-8,29,29,16,16,16,16,16,29,-17,-13,-15,29,-11,-30,16,-21,16,16,16,16,16,-12,-14,-10,-29,-22,-20,-23,-24,-25,-19,-16,]),'OP_MUL':([16,],[26,]),'OP_DIV':([16,],[27,]),'RPAREN':([11,13,15,17,21,22,24,30,31,32,33,34,35,36,37,38,39,40,41,42,46,47,48,49,50,51,52,53,54,55,56,57,58,],[18,-26,-28,-27,32,35,38,46,-17,-13,-15,48,-11,-30,50,-21,51,52,53,54,-12,-14,-10,-29,-22,-20,-23,-24,-25,57,58,-19,-16,]),'ASSIGN':([16,],[28,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'statement_list':([0,],[1,]),'declaration':([21,22,30,34,],[33,33,47,47,]),'import_statement':([0,1,],[6,7,]),'expression_list':([10,],[12,]),'program':([0,],[3,]),'parameter_list':([23,24,25,26,27,],[37,39,40,41,42,]),'type':([29,],[45,]),'declaration_list':([21,22,],[30,34,]),'expression':([10,12,23,24,25,26,27,37,39,40,41,42,43,],[14,20,36,36,36,36,36,49,49,49,49,49,55,]),'component':([0,1,],[5,8,]),}

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
