#ifndef AI_SERVING_CPP_MODEL_STATE_HPP
#define AI_SERVING_CPP_MODEL_STATE_HPP

#include <vector>
#include <string>
#include <inttypes.h>

class ModelState{
public:
    std::string tensors; 
    std::vector<uint32_t> ids; 
    std::vector<std::string> outputs; 

    void clear(){
        ids.clear();
        outputs.clear(); 
    }
};




#endif //AI_SERVING_CPP_MODEL_STATE_HPP