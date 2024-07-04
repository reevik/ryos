/// Node operation.
type OP<'a> = dyn Fn(&'a KeyData, &'a DataNode) -> &'a KeyData<'a>;
type KeyData<'a> = (&'a String, DataRecord<'a>);
type KeyNode<'a> = (&'a String, &'a dyn BaseNode<'a>);

struct BLinkTree<'a> {
    root: Option<&'a dyn BaseNode<'a>>,
}

impl<'a> BLinkTree<'a> {

    fn upsert(&'a self, query: &'a Query) -> Vec<&'a KeyData> {
        match self.root {
            Some(root) => {
                let add = |kd: &'a (&String, DataRecord), _dn: &'a DataNode| { 
                            return kd; 
                        };
                return root.query_and_execute(query,   &add);
            },
            None => vec![]
        }
    }

    fn query(&self, query: &'a Query) -> Vec<&KeyData> {
        match self.root {
            Some(root) => root.query_and_execute(query, &|kd, _dn| { return kd; }),
            None => vec![]
        }
    }

    fn new() -> BLinkTree<'a> {
        BLinkTree{root: None}
    }
}



enum NodeType {
    /// Represents the inner node of the index tree.
    INNER, 

    /// Data node which maintains a set of index key to data record mappings.
    DATA
}

struct DataRecord<'a> {
    data: &'a String,
}

struct Node<'a> {
    node_type: NodeType,
    index_key: String,
    parent: Option<&'a dyn BaseNode<'a>>,
    sibling: Option<&'a dyn BaseNode<'a>>,
}


// for now, we use a string object for the payload but in future is will be the blob representation.
// The query is the index key itself for the PoC.
struct Query<'a> {
    query_str: &'a String,
    payload: &'a String,
}

impl<'a> Query<'a> {
    // A naive implementation of index query.
    fn is_in_range_of(&self, index_key: &String) -> bool {
        self.query_str < index_key
    }

    fn is_matched(&self, index_key: &String) -> bool {
        self.query_str == index_key
    }
}

struct InnenNode<'a> {
    node: Node<'a>, 
    children: &'a Vec<&'a KeyNode<'a>>,
}

trait BaseNode<'a> {

    /// query and executes the operation provided on the current node.
    fn query_and_execute(&'a self, query_index_key: &'a Query, op: &'a dyn Fn(&'a KeyData, &'a DataNode) -> &'a KeyData<'a>) -> Vec<&'a KeyData>;

    /// Splits the current node into two nodes while registering the new node in the parent.
    /// The split operation is recursive. If the parent also reaches its maximum capacity, 
    /// it will continue to split including the root node.
    fn split(&'a self);
}

impl<'a> BaseNode<'a> for InnenNode<'a> {
    
    fn query_and_execute(&self, query_index_key: &'a Query, op: &'a dyn Fn(&'a KeyData, &'a DataNode) -> &'a KeyData<'a>) -> Vec<&'a KeyData> {
        for &key_node in self.children {
            if query_index_key.is_in_range_of(key_node.0) {
                return key_node.1.query_and_execute(query_index_key, op);
            }
        }
        match self.node.sibling {
            Some(n) => n.query_and_execute(query_index_key, op),
            _ => panic!("Don't expected to be there.!") 
        }
    }
    
    fn split(&self) {
        todo!()
    }
}

impl<'a> BaseNode<'a> for DataNode<'a> {
    
    fn query_and_execute(&'a self, query_index_key: &'a Query, op: &'a dyn Fn(&'a KeyData, &'a DataNode) -> &'a KeyData<'a>) -> Vec<&'a KeyData> {
        let results = self.children.iter()
            .filter(|&data_key| query_index_key.is_matched(data_key.0))
            .map( |data_key| { op(data_key, self)})
            .collect();

        return results;
        
    }
    
    fn split(&self) {
        todo!()
    }
}


struct DataNode<'a> {
    node: Node<'a>, 
    children: &'a mut Vec<KeyData<'a>>,
}

impl<'a> DataNode<'a> {

    fn add(& mut self, query: & Query<'a>) {
        let new_record: DataRecord = DataRecord{ data: query.payload };
        self.children.push((query.query_str, new_record));
    }
}


fn main() {
    let blinktree = BLinkTree::new();
    let query = Query{query_str: & "nope".to_string(), payload: &"".to_string() };
    let results = blinktree.query(&query);
    println!("Size of the result set={}", results.len())
}
