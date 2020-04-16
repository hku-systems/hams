

def funcname(self, parameter_list):
    pass


def get_all_nodes(dag_description):
    
    list = dag_description.split('\n')

    nodes_number = int(list[0])

    return list[2: 2+nodes_number]


def get_name_version(model_info):
    #print(model_name)
    list = model_info.split(',')
    #print(list)
    return list[0],list[1],list[2]

def is_stateful(model_info):
    list = model_info.split(',')
    if list[3] == "true":
        return True
    else:
        return False

def is_primary(model_info, container_name):
    list = model_info.split(',')
    index = list.index(container_name)
    if index < 10:
        return True
    else:
        return False
        
def get_batch_size(model_info):
    list = model_info.split(',')
    return int(list[4])


def expand_dag(dag_description, name, version, container_info, proxy_info, backup_info, frontend_info):

    ## create a new list
    new_list = []

    ## split original dag
    list = dag_description.split('\n')

    ## get node number
    nodes_number = int(list[0])

    new_list.append(name+'-'+version)
    new_list.append(list[0])
    new_list.append(list[1])
    new_list.append(','.join(frontend_info))

    for node_info, container_info, proxy_info, backup_info in zip(list[2: 2+nodes_number], container_info, proxy_info, backup_info):
        
        runtime_info = container_info+proxy_info+backup_info
        new_list.append(node_info+','+','.join(runtime_info))
        
    for edge_info in list[2+nodes_number:]:
        new_list.append(edge_info)

    expanded_dag = '\n'.join(new_list)

    #for info in new_list:
    #    expanded_dag = expanded_dag + info + "\n"

    return expanded_dag

def get_model_from_dag(dag_description, count):

    node_list = dag_description.split('\n')
    
    nodes_number = int(node_list[1])

    return node_list[4+count-1]


    #for node_info in node_list[2: 2+nodes_number]:
    #    infos = node_info.split(',')
    #    if infos[4] == containerid:
    #        return infos

def is_running(model_info, container_name):
    lis = model_info.split(',')
    #index = list.index(container_name)
    if container_name in lis:
        return True
    else:
        return False


def gen_new_runtime_dag(dag_description, model_name, model_version, isstateful, isprimary, container_name, container_id, container_ip, proxy_name, proxy_id, proxy_ip):

    new_list = []
    
    node_list = dag_description.split('\n')
    
    nodes_number = int(node_list[1])

    new_list.append(node_list[0])
    new_list.append(node_list[1])
    new_list.append(node_list[2])
    new_list.append(node_list[3])

    for node_info in node_list[4: 4+nodes_number]:
        #print("handling%s"%(node_info))
        infos = node_info.split(',')
        if infos[0] == model_name and infos[1] == model_version:

            new_instance = [container_name,container_id,container_ip,proxy_name,proxy_id,proxy_ip]
            info_list = []
            if isstateful:
                if isprimary:
                    ## Primary failure
                    info_list.append(model_name)
                    info_list.append(model_version)
                    info_list.extend(infos[2:5])
                    info_list.extend(infos[11:])
                    info_list.extend(new_instance)
                else:
                    ## Backup failure
                    info_list.append(model_name)
                    info_list.append(model_version)
                    info_list.extend(infos[2:5])
                    info_list.extend(infos[5:11])
                    info_list.extend(new_instance)

            else:
                info_list.append(model_name)
                info_list.append(model_version)
                info_list.extend(infos[2:5])
                info_list.extend(new_instance)
                 
            new_list.append(','.join(info_list))
        else:
            new_list.append(node_info)

    for edge_info in node_list[4+nodes_number:]:
        new_list.append(edge_info)

    expanded_dag = '\n'.join(new_list)

    return expanded_dag



                    
