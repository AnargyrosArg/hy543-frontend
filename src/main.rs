use std::collections::HashMap;
use std::fmt;

use std::error::Error;
use serde::{Serialize, Deserialize};
//use std::io::Write;

use std::net::TcpStream;
use std::io::Write;


// define the schema 
fn suggest_database_schema(user_schema: &HashMap<String, String>) -> String {
    // Column definitions will be stored here
    let mut column_definitions = Vec::new();

    // Define your data type mapping
    let data_type_map = HashMap::from([
        ("string".to_string(), "TEXT".to_string()),
        ("integer".to_string(), "INTEGER".to_string()),
        ("float".to_string(), "REAL".to_string()),
        ("boolean".to_string(), "BOOLEAN".to_string()),
    ]);

    // Iterate over the user schema
    for (column, data_type) in user_schema {
        let default_type = "TEXT".to_string(); // Create the default type
        let db_data_type = data_type_map
                .get(data_type)
                .unwrap_or(&default_type); // Use the reference

        column_definitions.push(format!("{} {}", column, db_data_type));
    }

    // Construct the CREATE TABLE statement
    let table_name = "user_data"; // Customize as needed
    format!(
        "CREATE TABLE {} ({});",
        table_name,
        column_definitions.join(", ")
    )
}

  
//let's say a dataframe
#[derive(Serialize, Deserialize, Debug)]
struct Dataframe {
    // data and schema fields
    //execution graph
    operations: Vec<OpNode>, 
}

//we have these operations
#[derive(Serialize, Deserialize, Debug)]
enum OperationType {
    Read, //filename
    Select,
    Where,
    Sum,
    Count,
    Empty, // it is for the initialization
}

// define the node 
#[derive(Serialize, Deserialize, Debug)]
struct OpNode {
    //id
    lazy: bool, // if it is lazy t means it just creates a new node to the graph
    function_name: OperationType,//the function name
    args: Vec<String>,//arguments about the function
    //to add more fields
}

// display the execution graph and the operation type of each node.
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationType::Select => write!(f, "Select"),
            OperationType::Where => write!(f, "Where"),
            OperationType::Sum => write!(f, "Sum"),
            OperationType::Count => write!(f, "Count"),
            OperationType::Empty => write!(f, "Empty"),
            OperationType::Read => write!(f, "Read"),
        }
    }
}

impl Dataframe {

    fn new() -> Self {
        Dataframe { 
            operations: vec![OpNode {
                lazy: false,
                function_name: OperationType::Empty,
                args: vec![],
            }],
        }
    }

    fn select(&mut self, arg: String) -> &mut Self {
        
        let arguments: Vec<String> = arg.split(" ").map(|s| s.to_string()).collect();
        
        let select_node = OpNode {
            lazy: true, 
            function_name: OperationType::Select,
            args: arguments, 
        };
        self.operations.push(select_node);
        self 
    }

    fn where_(&mut self, condition: String) -> &mut Self {

        let arguments: Vec<String> = condition.split(" ").map(|s| s.to_string()).collect();

        let where_node = OpNode {
            lazy: true,
            function_name: OperationType::Where,
            args: arguments, 
        };
        self.operations.push(where_node);
        self 
    }

    fn sum(&mut self, column: String) -> &mut Self {
        let sum_node = OpNode {
            lazy: false, 
            function_name: OperationType::Sum,
            args: vec![column],
        };
        self.operations.push(sum_node);
        self.send_graph();
        self
    }

    fn count(&mut self, column: String) -> &mut Self {
        let count_node = OpNode {
            lazy: true,
            function_name: OperationType::Count,
            args: vec![column],
        };
        self.operations.push(count_node);
        self.send_graph();
        self
    }

    fn read(&mut self, filename: String) -> &mut Self {
        let read_node = OpNode {
            lazy: false, 
            function_name: OperationType::Read,
            args: vec![filename],
        };
        self.operations.push(read_node);
        self
    }


    fn send_graph(&self){
        //--graph in json file--
        let json = serde_json::to_string(self).expect("couldnt serialize graph");
        //--send the execution graph via a socket
        //create a socket
        let mut stream = TcpStream::connect("127.0.0.1:8000").unwrap();
        let msg = json.as_bytes();
        stream.write(msg).unwrap();
    }

}


////////////////////////////////////////////////////////////////////////
/// 


fn main(){

    //dataframe
    let mut dataframe = Dataframe::new();

    //filename
    let filename: String = "deniro.csv".to_string();

    //--graph in memory--
    //the graph is vector of operation nodes.
    dataframe.read(filename.clone());
    dataframe.select("column1 column3".to_string());
    dataframe.where_("column2 > 10".to_string());           
    dataframe.sum("column1".to_string()); 
    dataframe.count("column1".to_string()); 

}



