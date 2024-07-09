use std::{cell::RefCell, rc::Rc};

/// Node operation.
type KeyData = (String, DataRecord);
type KeyNode = (String, TreeNode);

struct BLinkTree {
    root: Option<TreeNode>,
}

impl BLinkTree {

    fn upsert(& mut self, query: Query) -> Vec<&KeyData> {
        match &self.root {
            Some(root) => {
                match root {
                    TreeNode::INNER(n)=> {
                        n.query_and_execute(query, |kd: &KeyData, _dn: &DataNode| { 
                            let m = &n.children;
                            return kd; 
                        })
                    },
                    TreeNode::DATA(n) => { 
                        n.query_and_execute(query, |kd: &KeyData, _dn: &DataNode| { 
                            let abc = _dn.get();
                            return kd; 
                        })
                    }
                }
            },
            None => vec![]
        }
    }

    fn query(&self, query: Query) -> Vec<&KeyData> {
        match &self.root {
            Some(root) => {
                match root {
                    TreeNode::INNER(n)=> {
                        n.query_and_execute(query, |kd: &KeyData, _dn: &DataNode| { 
                            let m = &n.children;
                            return kd; 
                        })
                    },
                    TreeNode::DATA(n) => { 
                        n.query_and_execute(query, |kd: &KeyData, _dn: &DataNode| { 
                            let abc = _dn.get();
                            return kd; 
                        })
                    }
                }
            },
            None => vec![]
        }
    }

    fn new() -> BLinkTree {
        BLinkTree{root: None}
    }
}


enum NodeType {
    /// Represents the inner node of the index tree.
    INNER, 

    /// Data node which maintains a set of index key to data record mappings.
    DATA
}

struct DataRecord {
    data: String,
}

struct Node {
    node_type: NodeType,
    index_key: String,
    parent: Option<TreeNode>,
    sibling: Option<TreeNode>,
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

enum TreeNode {
    INNER(Box<InnerNode>), 
    DATA(Box<DataNode>),
}

struct InnerNode {
    node: Node, 
    children: Vec<KeyNode>,
}

trait BaseNode<'a> {

    /// query and executes the operation provided on the current node.
    fn query_and_execute(&'a self, query_index_key: Query, op: impl FnMut(&'a KeyData, &'a DataNode) -> &'a KeyData) -> Vec<&'a KeyData>;

    /// Splits the current node into two nodes while registering the new node in the parent.
    /// The split operation is recursive. If the parent also reaches its maximum capacity, 
    /// it will continue to split including the root node.
    fn split(&'a self);
}

impl<'a> BaseNode<'a> for InnerNode {
    
    fn query_and_execute(&'a self, query_index_key: Query, op: impl FnMut(&'a KeyData, &'a DataNode) -> &'a KeyData) -> Vec<&'a KeyData> {
        for key_node in &self.children {
            if query_index_key.is_in_range_of(&key_node.0) {
                match &key_node.1 {
                    TreeNode::INNER(n)=> {
                        return n.query_and_execute(query_index_key, op);
                    },
                    TreeNode::DATA(n) => { 
                        return  n.query_and_execute(query_index_key, op);
                    }
                }
            }
        }
        match &self.node.sibling {
            Some(n) => {
                match n {
                    TreeNode::INNER(n)=> {
                        return n.query_and_execute(query_index_key, op);
                    },
                    TreeNode::DATA(n) => { 
                        return  n.query_and_execute(query_index_key, op);
                    }                
                }
            },
            _ => panic!("Don't expected to be there.!") 
        }
    }
    
    fn split(&self) {
        todo!()
    }
}

impl<'a> BaseNode<'a> for DataNode {
    
    fn query_and_execute(&'a self, query_index_key: Query, mut op: impl FnMut(&'a KeyData, &'a DataNode) -> &'a KeyData) -> Vec<&'a KeyData> {
        let results = self.children.iter()
            .filter(|&data_key| query_index_key.is_matched(&data_key.0))
            .map( |data_key| { op(data_key, self)})
            .collect();

        return results;
        
    }
    
    fn split(&self) {
        todo!()
    }
}


struct DataNode {
    node: Node, 
    children: Vec<KeyData>,
}

impl DataNode {

    fn add(& mut self, query: &Query) {
        let new_record: DataRecord = DataRecord{ data: query.payload.to_string() };
        self.children.push((query.query_str.to_string(), new_record));
    }

    fn get(&self) -> &Vec<KeyData>{
        &self.children
    }
}


fn main() {
    let blinktree = BLinkTree::new();
    let query = Query{query_str: & "nope".to_string(), payload: &"".to_string() };
    let results = blinktree.query(query);
    println!("Size of the result set={}", results.len())
}
