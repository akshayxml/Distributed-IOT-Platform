# For now I am fxing the ip address of each node.
# all machines can perform password less ssh among each other

# all machines have common folder /datadrive --> created using nfs

#node1 = 'rootadmin@52.188.83.232'
host1 = '52.188.83.232'
#host1 = '20.51.210.82'
username1 = 'rootadmin'
#username1 = 'IAS-Node-3'
kafka_address = host1+':9092'

pathToPlatform = '/datadrive/bootstrap/ias-spring-2021-group-4/sample_application/'

def createDockerImage(ssh,ImageName,folderName): #path -> platformManager_docker
    dockerFilePath = pathToPlatform+folderName+'/dockerfile'
    pathToFolder = pathToPlatform+folderName+'/'
    command = 'docker build -t {} -f {} {}'.format(ImageName,dockerFilePath,pathToFolder)
    print('command is : ',command)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    print(ssh_stdout.readlines())

from paramiko import SSHClient
import paramiko

ssh1 = SSHClient()
ssh1.load_system_host_keys()
ssh1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh1.connect(hostname=host1,port=22,username=username1)


#build platform manager image
createDockerImage(ssh1,'platform-manager','platformManager_docker')

dockRunCommand = 'docker run -d --name platform-manager -p 5001:5001 -e KAFKA_ADDRESS={} platform-manager'.format(kafka_address)
ssh1.exec_command(dockRunCommand)