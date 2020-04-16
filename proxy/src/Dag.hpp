#ifndef DAG_HPP
#define DAG_HPP

#include <string> 
#include <vector> 


struct dag_proxy{
    std::string ip; 
    std::string container_name; 
    bool stateful; 
    std::string backup_ip;
    std::string backup_container_name;  


    int batch_size;
    bool dynamic_batch;

    std::string model_ip; 
    std::string model_container_name; 

    std::string backup_model_ip; 
    std::string backup_model_container_name;


}; 

struct dist_edge_t{
    int from; 
    std::vector<int> tos; 
    std::string to_string(){
        std::string ret = ""; 
        ret += std::to_string(from); 
        ret += " to "; 

        for (auto &to: tos){
            ret += std::to_string(to); 
            ret += " "; 
        }

        ret += "EOF"; 
        return ret; 
    }
};

struct reduce_edge_t{
    std::vector<int> froms; 
    int to; 

    std::string to_string(){
        std::string ret = ""; 
        

        for (auto &from: froms){
            ret += std::to_string(from); 
            ret += " "; 
        }
        ret += " to "; 

        ret += std::to_string(to); 

        ret += "EOF"; 
        return ret; 
    }
};

class DAG{

public:
    DAG()=default; 
    ~DAG()=default; 

    std::vector<dag_proxy> proxies; 
    std::vector<dist_edge_t> dist_eges; 
    std::vector<reduce_edge_t> reduce_edges; 
    
    std::string front_end_addr; 
    std::string dag_name; 
    
    //get Previous sStatFul Models and Next statFul Models
    //Note that **index start from 1, instead of 0**
    std::vector<int> get_PFM(int myid);     
    std::vector<int> get_NFM(int myid); 


    void parse(std::string dag_str);
};



#endif //DAG_HPP
