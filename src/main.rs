use std::fmt;

use serde::{Deserialize, Serialize};
//use std::io::Write;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

//let's say a dataframe
#[derive(Serialize, Deserialize, Debug)]
struct Dataframe {
    // data and schema fields
    //execution graph
    operations: Vec<OpNode>,
    checkpoint: usize
}

//we have these operations
#[derive(Serialize, Deserialize, Debug)]
enum OperationType {
    Read, //filename
    Select,
    Where,
    Sum,
    Count,
    Fetch,
    Empty, // it is for the initialization
}

// define the node
#[derive(Serialize, Deserialize, Debug)]
struct OpNode {
    //id
    id: usize, // if it is lazy t means it just creates a new node to the graph
    function_name: OperationType, //the function name
    args: Vec<String>, //arguments about the function
               //to add more fields
}

// display the execution graph and the operation type of each node.
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationType::Select => write!(f, "Select"),
            OperationType::Fetch => write!(f, "Fetch"),
            OperationType::Where => write!(f, "Where"),
            OperationType::Sum => write!(f, "Sum"),
            OperationType::Count => write!(f, "Count"),
            OperationType::Empty => write!(f, "Empty"),
            OperationType::Read => write!(f, "Read"),
        }
    }
}

static mut ID: usize = 0;
impl Dataframe {
    fn new() -> Self {
        Dataframe {
            operations: vec![OpNode {
                id: 0,
                function_name: OperationType::Empty,
                args: vec![],
            }],
            checkpoint: 0,
        }
    }

    fn select(&mut self, arg: String) -> &mut Self {
        let arguments: Vec<String> = arg.split(" ").map(|s| s.to_string()).collect();
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        let select_node = OpNode {
            id: current_id,
            function_name: OperationType::Select,
            args: arguments,
        };
        self.operations.push(select_node);
        self
    }

    fn where_(&mut self, condition: String) -> &mut Self {
        let arguments: Vec<String> = condition.split(" ").map(|s| s.to_string()).collect();
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        let where_node = OpNode {
            id: current_id,
            function_name: OperationType::Where,
            args: arguments,
        };
        self.operations.push(where_node);
        self
    }

    fn sum(&mut self, column: String) -> &mut Self {
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        print!("Sum of collumn {}: ", column);
        let sum_node = OpNode {
            id: current_id,
            function_name: OperationType::Sum,
            args: vec![column],
        };
        self.operations.push(sum_node);
        self.send_graph();
        self.get_response();
        println!("");
        //clear graph, its already been executed at this point
        self.operations = vec![];
        //mark latest operation executed id
        self.checkpoint = current_id;
        self
    }

    fn count(&mut self) -> &mut Self {
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        let count_node = OpNode {
            id: current_id,
            function_name: OperationType::Count,
            args: vec![],
        };
        self.operations.push(count_node);
        self.send_graph();
        print!("Counted ");
        self.get_response();
        print!(" total rows\n");
        
        //clear graph, its already been executed at this point
        self.operations = vec![];
        //mark latest operation executed id
        self.checkpoint = current_id;
        self
    }

    fn read(&mut self, filename: String) -> &mut Self {
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        let read_node = OpNode {
            id: current_id,
            function_name: OperationType::Read,
            args: vec![filename],
        };
        //lazy operation -> just append to graph
        self.operations.push(read_node);
        self
    }

    fn fetch(&mut self) -> &mut Self {
        unsafe { ID = ID+1 };
        let current_id = unsafe { ID };
        let fetch_node = OpNode {
            id: current_id,
            function_name: OperationType::Fetch,
            args: vec![],
        };
        //add node to graph
        self.operations.push(fetch_node);
        //send graph for execution
        self.send_graph();
        //get response
        self.get_response();
        //clear graph, its already been executed at this point
        self.operations = vec![];
        //mark latest operation executed id
        self.checkpoint = current_id;
        self
    }

    fn send_graph(&self) {
        //--graph in json file--
        let json = serde_json::to_string(self).expect("couldnt serialize graph");
        //--send the execution graph via a socket
        //create a socket
        
        let communicator_addr ="mpi0:65000";

        let mut stream = TcpStream::connect(communicator_addr).unwrap();
        let msg = json.as_bytes();
        stream.write(msg).unwrap();
    }

    fn get_response(&self) {
        let client_addr = "0.0.0.0:65001";
        let listener = TcpListener::bind(client_addr).unwrap();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();

            let mut buffer = [0; 512];
            let read_size = stream.read(&mut buffer).unwrap();
            let s = std::str::from_utf8(&buffer[0..read_size])
                .expect("Not a valid UTF-8 sequence")
                .to_string();

            print!("{}", s);
            break;
        }
    }
}


fn main() {
    //dataframe
    let mut dataframe = Dataframe::new();

    //filename

    //--graph in memory--
    //the graph is vector of operation nodes.
    dataframe.read("deniro.csv".to_string());
    dataframe.select("Year Title".to_string());
    dataframe.where_("Score > 90".to_string());
    dataframe.sum("Year".to_string());
    dataframe.sum("Year".to_string());
    dataframe.sum("Score".to_string());
    // dataframe.fetch();
    dataframe.count();
}
