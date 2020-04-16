#include "frontend.hpp"
#include <thread>
#include <sys/time.h>

void ClientReq::Proceed(){
    if (status_ == CREATE) {
        status_ = PROCESS;
        
        service_->Requestdownstream(&ctx_, &request_, &responder_, cq_, cq_, this);

    } 
    else if (status_ == PROCESS) {
        new ClientReq(service_, cq_, frontend_);

        struct timeval tv1;
        gettimeofday(&tv1, NULL);
        
        
        modeltest::response response; 

        uint seq = ++frontend_->MaxSeq;
        
        prediction::request req; 

        

        req.mutable_input_()->CopyFrom(request_.input_());
        req.set_src_uri("front-end");
        req.set_seq(seq) ;
        req.set_req_id(frontend_->MaxSeq);

        DLOG(INFO) << "passing req to entry proxy, " << seq; 



        auto cv = std::make_shared<Output>();
        cv->finished = false; 

        std::unique_lock<std::mutex> lk(frontend_->mu); 
        frontend_->outputs[seq] = cv; 
        lk.unlock(); 


        //step 1. send two proxy
        grpc::ClientContext context; 
        grpc::Status status = frontend_->stub->downstream(&context, req,  &response); 

        if (! status.ok()){
            LOG(FATAL) << "Error calling entry proxy, code " << status.error_code(); 
        }





        lk.lock(); 
        if (frontend_->outputs[seq]->finished == true){
            lk.unlock(); 
        }else{
            frontend_->outputs[seq]->cond.wait(lk); 
        }
        
        //step 2. wait for response from last proxy. ; 

        DLOG(INFO) << "waiting thread received output, " << seq; 


        struct timeval tv2;
        gettimeofday(&tv2, NULL);

        int time_ms = (tv2.tv_sec - tv1.tv_sec) * 1000 + (tv2.tv_usec - tv1.tv_usec) / 1000; 

        response.set_status("req sequence: " + std::to_string(seq) + "; time: "+ std::to_string(time_ms)+ "ms; OUPUT (starting from next line):\n" + cv->output_); 


        status_ = FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } 
    else {
        delete this;
    }
}



void Result::Proceed(){
    if (status_ == CREATE) {
        status_ = PROCESS;
        
        service_->Requestoutputstream(&ctx_, &request_, &responder_, cq_, cq_, this);

    } 
    else if (status_ == PROCESS) {
        new Result(service_, cq_, frontend_);


        uint seq = request_.seq(); 
        DLOG(INFO) << "Received result from last proxy, seq" << seq; 

        std::unique_lock<std::mutex> lk(frontend_->mu); 
        if (frontend_->outputs.count(seq) == 0){
            LOG(FATAL) << "something wrong, received an output with no waiting thread" << seq; 
        }else{
            frontend_->outputs[seq]->finished = true; 
            frontend_->outputs[seq]->output_ = request_.input_().inputstream(); 
        }
        lk.unlock(); 
        
        frontend_->outputs[seq]->cond.notify_one(); 
        DLOG(INFO) << "Notified the waiting thread, seq" << seq; 


        response.set_status(std::to_string(seq)); 

        status_ = FINISH;
        responder_.Finish(response, grpc::Status::OK, this);
    } 
    else {
        delete this;
    }
}


void Frontend::run(){
    grpc::ServerBuilder builder; 

    builder.AddListeningPort("[::]:22223",  grpc::InsecureServerCredentials());
    
    builder.RegisterService(&service_);

    cq_client_ = builder.AddCompletionQueue();
    cq_result_ = builder.AddCompletionQueue();

    server_ = builder.BuildAndStart();

    DLOG(INFO) << "Front end server stared"; 

    
    std::vector<std::thread> vs; 
    for (int i = 0; i<n_workers; i++){
        vs.push_back(std::thread(&Frontend::handle_client, this)); 
    }


    
    handle_result(); 

}

void Frontend::handle_client(){
    DLOG(INFO) << "handling client request"; 

    new ClientReq(&service_, cq_client_.get(), shared_from_this());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      if(!cq_client_->Next(&tag, &ok)){
          LOG(FATAL) << "server shutdown" ; 
      }
      if (!ok){
          LOG(FATAL) << "something wrong";
      }
      static_cast<ClientReq*>(tag)->Proceed();
    }
}

void Frontend::handle_result(){
    DLOG(INFO) << "handling chain results"; 


    new Result(&service_, cq_result_.get(), shared_from_this());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      if(!cq_result_->Next(&tag, &ok)){
          LOG(FATAL) << "server shutdown" ; 
      }
      if (!ok){
          LOG(FATAL) << "something wrong";
      }
      static_cast<Result*>(tag)->Proceed();
    }
}