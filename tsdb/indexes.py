import pickle
import os
import bintrees

class Index:

  def __init__(self, field, db_name)
    self.db_name = db_name
    self.field = field
    self.file = db_name + '/' + field + '.idx'
    self.index = self.load_from_file

class BinTreeIndex(Index):

  def load_from_file(self):
    if os.path.exists(self.file):
      with open(self.file, 'rb') as f:
        return pickle.load(f)
    else:
      return bintrees.FastAVLTree()

  def save_to_file(self):
    with open(self.file, 'wb') as f:
      pickel.dump(self.index, f)

  def insert(self, field, pk):
    if field in self.tree:
      if pk not in self.tree[field]:
        self.index[field] = self.tree[field] + [pk]
    else:
        self.index[field] = [pk]

  def remove(self, field, pk):
    if field in self.tree:
      pks = self.index[field]
      if pk in pks:
        del pks[pks.index(pk)]
        self.index[field] = pks 
      else: 
        raise ValueError("Primary key is not in index")
    else:
      raise ValueError("Field not in index")

  def get(self, field):
    if field in self.index:
      return self.index[field]
    else:
      return set()

class BitMapIndex(Index):
    
  def load_from_file(self):
    if os.path.exist(self.file):
      with open(self.file) as fdi:
        items = [l.strip().split(':') for l in fdi.readlines()]
        self.index = {k: int(v) for k,v in items}


  def set(self, k, v):
    if

    



