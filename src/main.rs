use std::cell::RefCell;

/// Node operation.
type OP<'a> = fn(&KeyData, &DataNode) -> &'a KeyData<'a>;
type KeyData<'a> = (&'a String, DataRecord<'a>);
type KeyNode<'a> = (&'a String, &'a dyn BaseNode<'a>);

enum NodeType {
    INNER,
    DATA,
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
    children: RefCell<Vec<&'a KeyNode<'a>>>,
}

trait BaseNode<'a> {
    fn query_and_execute(&self, query_index_key: &Query, op: OP<'a>) -> Vec<&KeyData>;
}

impl<'a> BaseNode<'a> for InnenNode<'a> {
    fn query_and_execute(&self, query_index_key: &Query, op: OP<'a>) -> Vec<&KeyData> {
        let it = self.children.borrow();
        for &key_node in self.children.borrow().iter() {
            if query_index_key.is_in_range_of(key_node.0) {
                return key_node.1.query_and_execute(query_index_key, op);
            }
        }
        match self.node.sibling {
            Some(n) => n.query_and_execute(query_index_key, op),
            _ => panic!("Don't expected to be there.!"),
        }
    }
}

impl<'a> BaseNode<'a> for DataNode<'a> {
    fn query_and_execute(&self, query_index_key: &Query, op: OP<'a>) -> Vec<&KeyData> {
        self.children
            .borrow()
            .iter()
            .filter(|&data_key| query_index_key.is_matched(data_key.0))
            .map(|data_key| op(data_key, self))
            .collect()
    }
}

struct DataNode<'a> {
    node: Node<'a>,
    children: RefCell<Vec<KeyData<'a>>>,
}

impl<'a> DataNode<'a> {
    fn add(&mut self, query: Query<'a>) {
        let new_record: DataRecord = DataRecord {
            data: query.payload,
        };
        self.children
            .borrow_mut()
            .push((query.query_str, new_record));
    }
}

fn main() {
    print!("Hello World!");
}
