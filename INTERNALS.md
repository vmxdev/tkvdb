# Some implementation notes

## How put() function works, all possible combinations of keys and prefixes

Nodes layout

![database keys](docs/intr_put1.png?raw=true "database keys")

Figures below shows only affected nodes

Initial state (we have key "PREFIX" with value "VAL")

![node before update](docs/intr_put2.png?raw=true "node before update")

  * Simplest case: `transaction->put("PREFIX", "NEW VAL")` - update existing key. If size of new value is the same, nothing changed in nodes layout. Only old value replaced by new one. If size of new value differs, new node (with new value) will be created

  * `transaction->put("PRE", "VAL")` - will create two nodes

![node prefix -> pre + fix](docs/intr_put3.png?raw=true "node prefix -> pre + fix")

 * `transaction->put("PREFIXNEW", "VAL")` - will create two nodes

![node prefix -> prefix + prefixnew](docs/intr_put4.png?raw=true "node prefix -> prefix + prefixnew")

 * `transaction->put("PREPARE", "VAL")` - will create three nodes

![node prefix -> prefix + prepare](docs/intr_put5.png?raw=true "node prefix -> prefix + prepare")
