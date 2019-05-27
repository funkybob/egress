
def register_type(obj, scope=None):
    print("register_type: %r %r" % (obj, scope))
    raise

def new_array_type(oids, name, base_caster):
    print("new_array_type: %r %r %r" % (oids, name, base_caster))
    raise
