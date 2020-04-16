#include "Dag.hpp"
#include <iostream>
#include <sstream> 
#include <glog/logging.h>
#include <stack>
#include <set>


    /*
     * c1,test,ai-proxy:echo-model,c1_test-89936,3b2b24120e,172.18.0.4,c1_test-89936-proxy,f43be85cd4,172.18.0.5
    */

void DAG::parse(std::string dag_str){
    std::istringstream f(dag_str);

    std::string line;


    //parse DAG
    std::getline(f, line);
    dag_name = line;
    DLOG(INFO) << "DAG NAME" << dag_name;

    std::getline(f, line);
    int n_proxy = std::stoi(line);

    std::getline(f, line);
    int n_edge = std::stoi(line);


    std::getline(f, line); 
    std::size_t current, previous = 0;
    //frontend name
    current = line.find(',');
    
    //frontEnd id
    previous = current + 1;
    current = line.find(',', previous);

    //frontEnd ip 
    previous = current + 1;
    current = line.find(',', previous);
    this->front_end_addr = line.substr(previous, current-previous);
    DLOG(INFO) << "Front end addr" << this->front_end_addr; 

    for (int i = 0; i < n_proxy; i++){

        dag_proxy p; 

        std::getline(f, line);

        std::size_t current, previous = 0;

        //C1
        current = line.find(',');

        //test
        previous = current + 1;
        current = line.find(',', previous);

        //model-container-image: ai-proxy:echo-model
        previous = current + 1;
        current = line.find(',', previous);

        //stateful, true/false
        previous = current + 1; 
        current = line.find(',', previous); 
        auto stateful = line.substr(previous, current-previous);
        if (stateful == "true"){
            p.stateful = true; 
        }else{
            p.stateful = false; 
        }


        //batch_sizes.
        previous = current + 1; 
        current = line.find(',', previous); 
        p.batch_size = std::stoi(line.substr(previous, current-previous));
        if (p.batch_size < 0){
            p.dynamic_batch = true;
            p.batch_size = 0 - p.batch_size;
        }else{
            p.dynamic_batch = false;
        }

        //model-container-name: c1_test-89936
        previous = current + 1;
        current = line.find(',', previous);
        p.model_container_name = line.substr(previous, current-previous); 

        //model-container-id: 3b2b24120e
        previous = current + 1;
        current = line.find(',', previous);

        //model_ip: 172.18.0.4
        previous = current + 1;
        current = line.find(',', previous);
        p.model_ip = line.substr(previous, current-previous); 

        //proxy_contianer_name: c1_test-89936-proxy
        previous = current + 1;
        current = line.find(',', previous);
        p.container_name = line.substr(previous, current-previous);
        

        //proxy_container_id: f43be85cd4:
        previous = current + 1;
        current = line.find(',', previous);

        //proxy_ip: f43be85cd4:
        previous = current + 1;
        current = line.find(',', previous);
        p.ip = line.substr(previous, current-previous);

        DLOG(INFO) << "SetDAG: proxy "<< i+1 <<" ip = " << p.ip << "name = " << p.container_name << "batch = " << p.batch_size; 

        if (p.stateful){
            //bakckup info
            //model-container-name: c1_test-89936
            previous = current + 1;
            current = line.find(',', previous);
            p.backup_model_container_name = line.substr(previous, current-previous); 


            //model-container-id: 3b2b24120e
            previous = current + 1;
            current = line.find(',', previous);

            //model_ip: 172.18.0.4
            previous = current + 1;
            current = line.find(',', previous);
            p.backup_model_ip = line.substr(previous, current-previous); 


            //proxy_contianer_name: c1_test-89936-proxy
            previous = current + 1;
            current = line.find(',', previous);
            p.backup_container_name = line.substr(previous, current-previous);
            

            //proxy_container_id: f43be85cd4:
            previous = current + 1;
            current = line.find(',', previous);

            //proxy_ip: f43be85cd4:
            previous = current + 1;
            current = line.find(',', previous);
            p.backup_ip = line.substr(previous, current-previous);
        
            DLOG(INFO) << "------: backup proxy "<< i+1 <<" ip = " << p.backup_ip << "name = " << p.backup_container_name; 
        }

        this->proxies.push_back(p); 
    }

    //Analyzing edge. 
    DLOG(INFO) << "going to parse edges, total number of line" << n_edge; 
    for (int i = 0; i< n_edge; i++){
        std::getline(f, line);
        
        std::size_t current, previous = 0;

        current = line.find('[', previous);

        auto type = line.substr(previous, current-previous);

        previous = current + 1; 
        current = line.find(']', previous);

        auto content = line.substr(previous, current-previous);

        DLOG(INFO) << "Parsing the " << i <<"th line of the edges, type ="<<type<<"EOF, content="<<content <<"EOF"; 
        previous = 0; 
        current = 0; 
        
        if (type == "dist"){
            while(current != std::string::npos){
                current = content.find(';', previous);
                std::string edge;
                if (current != std::string::npos){
                    edge = content.substr(previous, current-previous);
                }else{
                    edge = content.substr(previous);
                }

                DLOG(INFO) << "Analyzing edge:" << edge <<"END";
                dist_edge_t d; 

                size_t connector = edge.find('-', 0); 

                d.from = std::stoi(edge.substr(0, connector)); 

                size_t to_current = 0; 
                size_t to_previous = connector+2; 
                
                while(to_current != std::string::npos){
                    to_current = edge.find(',', to_previous); 
                    if (to_current != std::string::npos){
                        d.tos.push_back(std::stoi(edge.substr(to_previous, to_current-to_previous)));
                    }else{
                        d.tos.push_back(std::stoi(edge.substr(to_previous)));
                    }
                    to_previous = to_current+1; 
                }


                LOG(INFO) << "Dist edge, from " << d.from << " tos size " << d.tos.size(); 
                for (auto &to : d.tos){
                    LOG(INFO) << "---------: from " << d.from << " to " << to; 
                } 

                this->dist_eges.push_back(d); 

                previous = current + 1; 
            }
        }

        else if (type == "reduce"){
            while(current != std::string::npos){
                current = content.find(';', previous); 
                std::string edge;

                if (current != std::string::npos){
                    edge = content.substr(previous, current-previous);
                }else{
                    edge = content.substr(previous);
                }
                DLOG(INFO) << "Analyzing edge:" << edge <<"END";



                reduce_edge_t r; 

                size_t connector = edge.find('-', 0); 

                r.to = std::stoi(edge.substr(connector+2, std::string::npos-connector-2)); 

                size_t from_current = 0; 
                size_t from_previous = 0; 

                auto from_str = edge.substr(0, connector); 

                
                while(from_current != std::string::npos){
                    from_current = from_str.find(',', from_previous); 

                    if (from_current != std::string::npos){
                        r.froms.push_back(std::stoi(from_str.substr(from_previous, from_current-from_previous)));
                    }else{
                        r.froms.push_back(std::stoi(from_str.substr(from_previous)));
                    }

                    from_previous = from_current+1; 
                }


                LOG(INFO) << "Reduce edge, froms size " << r.froms.size() << " to " << r.to; 
                for (auto &from : r.froms){
                    LOG(INFO) << "-----------: from " << from << " to " << r.to; 
                } 

                this->reduce_edges.push_back(r); 

                previous = current + 1; 
            }
        }

    }

}

std::vector<int> DAG::get_PFM(int myid){
  std::vector<int> PFMs;
  
  std::stack<int> st; 
  std::set<int> visited; 

  st.push(myid);

  while(!st.empty()){
    int cur = st.top();
    st.pop();
    for (auto &e : dist_eges){  
      for (auto &to: e.tos){
        if (to == cur){
          if (visited.count(e.from) == 0){
            //not visited yet
            visited.insert(e.from);
            if (proxies[e.from-1].stateful){
              PFMs.push_back(e.from);
            }
            else{
              st.push(e.from);
            }
          }
        } 
      }
    }

    for (auto &e: reduce_edges){
      if (e.to == cur){
        for (auto &from : e.froms){
          if (visited.count(from) == 0){
            visited.insert(from);
            if (proxies[from-1].stateful){
              PFMs.push_back(from);
            }
            else{
              st.push(from);
            }
          }
        }
      }
    }
  }
}

std::vector<int> DAG::get_NFM(int myid){
  std::vector<int> NFMs;
  
  std::stack<int> st; 
  std::set<int> visited; 

  st.push(myid);

  while(!st.empty()){
    int cur = st.top();
    st.pop();
    for (auto &e : dist_eges){  
      if (e.from == cur){
        for (auto &to: e.tos){
          if (visited.count(to) == 0){
            visited.insert(to);
            if (proxies[to-1].stateful){
              NFMs.push_back(to); 
            }
            else{
              st.push(to);
            }
          }
        }
      }
    }

    for (auto &e: reduce_edges){
      for (auto &from : e.froms){
        if (from == cur){
          if (visited.count(e.to) == 0){
            visited.insert(e.to);
            if (proxies[e.to -1].stateful){
              NFMs.push_back(e.to); 
            }
            else{
              st.push(e.to);
            }
          }
        }
      }
    }
  }
}
