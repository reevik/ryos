enum NodeType {
    INNER, 
    DATA
}

struct DataRecord {
    data: String,
}

type KeyData<'a> = (&'a String, &'a DataRecord);
type KeyNode<'a> = (&'a String, &'a dyn BaseNode<'a>);

struct Node<'a> {
    node_type: NodeType,
    index_key: String,
    parent: Option<&'a dyn BaseNode<'a>>,
    sibling: Option<&'a dyn BaseNode<'a>>,
}

struct InnenNode<'a> {
    node: Node<'a>, 
    children: &'a Vec<&'a KeyNode<'a>>,
}

trait BaseNode<'a> {
    fn query_and_execute(&self, query_index_key: &String, op: Fn(&KeyData, &DataNode) -> &'a KeyData<'a>) -> Vec<&DataRecord>;
}

impl<'a> BaseNode<'a> for InnenNode<'a> {
    
    fn query_and_execute(&self, query_index_key: &String, op: Fn(&KeyData, &DataNode) -> &'a KeyData<'a>) -> Vec<&DataRecord> {
        for &key_node in self.children {
            let node_index_key = key_node.0;
            if node_index_key > query_index_key {
                return key_node.1.query_and_execute(query_index_key, op);
            }
        }
        match self.node.sibling {
            Some(n) => n.query_and_execute(query_index_key, op) ,
            _ => panic!("Test!") 
        }
    }
}

impl<'a> BaseNode<'a> for DataNode<'a> {
    
    fn query_and_execute(&self, query_index_key: &String, op: Fn(&KeyData, &DataNode) -> &'a KeyData<'a>) -> Vec<&DataRecord> {
        let results: Vec<&KeyData> = self.children.iter()
            .filter(|&&data_key| data_key.0 == query_index_key)
            .map( |&data_key| -> operation(data_key, self) ).collect();
        return results;
    }
}

struct DataNode<'a> {
    node: Node<'a>, 
    children: &'a Vec<&'a KeyData<'a>>,
}

fn main() {

    print!("Hello World!");
}
