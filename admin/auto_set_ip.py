import glob, os
import socket

def get_host_ip():
	 try:
	 	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	 	s.connect(('8.8.8.8', 80))
	 	ip = s.getsockname()[0]
	 finally:
	 	s.close
	 return ip

if __name__ == '__main__':
	ip = get_host_ip()
	print("IP = ", ip)

	hosts = open("./host_list", "r")
	num_host = int(hosts.readline())
	host_lst = []
	for i in range(num_host):
		host_lst.append(tuple(hosts.readline().rstrip().split(':')))
	hosts.close()

	target = "hams_conn.connect_host"
	for file in glob.glob("cluster_*"):
		#read the file
		file_cont = open(file, "r")
		file_st = file_cont.read().split("\n")
		file_cont.close()
		#replace the hams_conn.connect_host with the ones in host_lst
		connect_cmd = [target+"(\""+eachhost[0]+"\", \""+eachhost[1]+"\")" for eachhost in host_lst if "stop_all" in file or eachhost[0]!= ip]
		idxs = [file_st.index(st) for st in file_st if target in st]
		iden = file_st[idxs[0]].split(target)[0]
		for idx in idxs:
			file_st.pop(idx-idxs.index(idx))
		for cmd in connect_cmd:
			file_st.insert(idxs[0], iden+cmd)


		print("FILENAME: "+file+"\n"+"-"*40)
		file_cont = open(file,"w")
		file_cont.write("\n".join(file_st))
		file_cont.close()
