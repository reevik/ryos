use std::{cell::RefCell, rc::Rc};

/// Node operation.
type KeyData = (String, DataRecord);
type KeyNode = (String, TreeNode);

struct BLinkTree {
    root: Option<TreeNode>,
}

impl BLinkTree {
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

impl InnerNode {

    fn locate_range_key(&mut self, query_index_key: &Query) -> Option<&mut KeyNode> {
        self.children.iter_mut()
            .find(|child| query_index_key.is_in_range_of(&child.0))
    }

    fn query_thru_sibling(&mut self, query_index_key: &Query) -> &mut DataNode {
        match &mut self.node.sibling {
            Some(s) => match s {
                TreeNode::INNER(i) => i.query_datanode(query_index_key),
                _ => panic!("Not expected to find a data node sibling!")
            },
            // Sibling doesn't exist, so we reached the most right branch of the tree.
            None => self.query_thru_last(query_index_key),  
        }
    }

    fn query_thru_last(&self, query_index_key: &Query) -> &mut DataNode {
        match self.children.last() {
            Some(s) => match &s.1 {
                TreeNode::INNER(i) => i.query_datanode(query_index_key),
                _ => panic!("Not expected to find a data node sibling!")
            },
            _ => panic!("Not expected to find a data node sibling!"),
        }
    }

    fn query_datanode(&mut self, query_index_key: &Query) -> &mut DataNode {
        match self.locate_range_key(query_index_key) {
            Some(rk) => match &mut rk.1 {
                TreeNode::INNER(ic) =>  ic.query_datanode(query_index_key), 
                TreeNode::DATA(dc) => dc,
            },
            None => self.query_thru_sibling(query_index_key), 
        }
    }
}

pub trait BaseNode {
    /// query and executes the operation provided on the current node.
    fn query(&self, query_index_key: &Query) -> Vec<&KeyData>;
}

impl BaseNode for InnerNode {
    
    fn query(&self, query_index_key: &Query) -> Vec<&KeyData> {
       return self.query_datanode(query_index_key).query(query_index_key);
    }
}

impl BaseNode for DataNode {
    
    fn query(&self, query_index_key: &Query) -> Vec<&KeyData> {
        let matches: Vec<&KeyData> = self.children.iter()
            .filter(|child| query_index_key.is_matched(&child.0))
            .collect();

        return matches;
    }
}


struct DataNode {
    node: Node, 
    children: Vec<KeyData>,
}

impl DataNode {

    fn add(&mut self, query: &Query) {
        let new_record: DataRecord = DataRecord{ data: query.payload.to_string() };
        self.children.push((query.query_str.to_string(), new_record));
    }
}


fn main() {
}
