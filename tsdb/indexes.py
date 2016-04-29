class BinTreeIndex:

  def __init__(self, field, db_name)
    self.db_name = db_name
    self.field = field
    self.file = db_name + '/' + field + '.idx'
    self.tree = self.load_from_file

  def load_from_file(self):
    if os.path.exists(self.file):
      with open(self.file, 'rb') as f:
        return pickle.load(f)
    else:
      return bintrees.AVLTree()

  def save_to_file(self):
    with open(self.file, 'wb') as f:
      pickel.dump(self.tree, f)

  def insert(self, field, pk):
    pass

  def remove(self, field, pk):
    pass

  def get(self, field):

    if field in self.tree:
      return self.tree[field]
    else:
      return set()



