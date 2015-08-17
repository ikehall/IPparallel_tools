import jinja2 as jin
import uuid
import subprocess
import os
import time
import IPython.parallel as parallel
import shutil

ipcluster_template = jin.Template('''
c = get_config()
c.IPClusterStart.controller_launcher_class = 'LocalControllerLauncher'
c.IPClusterStart.engine_launcher_class = 'SSHEngineSetLauncher'
c.IPClusterStart.work_dir = '{{work_dir}}'
c.IPClusterStart.profile = '{{profile_name}}'
c.SSHEngineSetLauncher.engines = {{node_dict}}
''')

#Note, if you want to use a task database, this is what to edit
ipcontroller_template = jin.Template('''
c = get_config()
c.HubFactory.ip = u'*'
''')

ipengine_template = jin.Template('''
c = get_config()
''')

class Cluster(object):
    def __init__(self, nodes=None, engines=None, work_dir=None, profile=None, node_file=None):
        self.profile_name = 'temp_'+str(uuid.uuid1()) if profile is None else profile
        self.nengines = 0
        self.work_dir = work_dir if work_dir is not None else u'.'
        self.ipythondir = os.path.join(os.environ['HOME'], '.ipython') if 'IPYTHONDIR' not in os.environ else os.environ['IPYTHONDIR']
        self.profile_dir = os.path.join(self.ipythondir, 'profile_'+str(self.profile_name))

        #Does the profile exist already?
        #If so, ignore everything else and just launch the cluster
        if os.path.exists(self.profile_dir):
            #Profile exists!  Determine number of engines for proper functioning of
            #wait_for_cluster
            with open(os.path.join(self.ipythondir, 'profile_'+str(self.profile_name),'ipcluster_config.py')) as f:
                for line in f:
                    #replace this stuff with a regex later
                    if 'SSHEngineSetLauncher.engines' not in line:
                        continue
                    pnodes = line.split('=')[1].strip(' {}').split()
                    for node in pnodes:
                        self.nengines += int(node.split(':')[1].strip(','))
                        
        #Profile is not pre-existing:  Do we have a node file?
        elif node_file is not None:
            #We've passed a file containg lines of the format:
            # node <nengines>
            self.node_dict = {}
            with open(node_file) as f:
                for line in f:
                    #Replace this stuff with a regex later
                    n = line.split(' ')
                    if len(n) < 2:
                        continue
                    if '#' in n[0]:
                        #Cut out comments
                        continue
                    self.node_dict[n[0]] = int(n[1])
                    self.nengines += int(n[1])
            self._create_profile()
            
        #Do we have lists of nodes and engines?
        elif nodes is not None and engines is not None and len(nodes)==len(engines):
            self.node_dict = {}
            for i, node_name in enumerate(nodes):
                self.node_dict[node] = int(engines[i])
                self.nengines += int(engines[i])
            self._create_profile()
            
        else:
            #We have not properly specified nodes/engines for a parallel cluster
            #Create a "cluster" only on localhost.  Number of engines will be
            #The number of cpus on the local machine
            import multiprocessing
            self.nengines = multiprocessing.cpu_count()
            self.node_dict = {}
            self._create_profile()

        self.start_cluster()

        
        

    def _create_profile(self):
        #Check to see if profile_name exists in users ipython directory
        
        args = ['ipython','profile','create','--parallel','--profile='+str(self.profile_name)]
        print args
        subprocess.call(args)
        #Build the strings for the templates
        cluster_f = ipcluster_template.render(
            {'work_dir':self.work_dir,
             'profile_name':self.profile_name,
             'node_dict':self.node_dict})
        controller_f = ipcontroller_template.render({})
        engine_f = ipengine_template.render({})
        #Overwrite the created files
        
        with open(os.path.join(self.profile_dir,
                               'ipcluster_config.py'),
                  'w') as f:
            f.write(cluster_f)

        with open(os.path.join(self.profile_dir,
                               'ipcontroller_config.py'),
                  'w') as f:
            f.write(controller_f)
            
        with open(os.path.join(self.profile_dir,
                               'ipengine_config.py'),
                  'w') as f:
            f.write(engine_f)

    def start_cluster(self):
        cmd = ['ipcluster','start','--profile='+self.profile_name]
        clstr = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 preexec_fn=os.setsid)
        time.sleep(1)
        self._wait_for_cluster(120)
        
    def _wait_for_cluster(self, timeout):
        tic = time.time()
        #Wait to connect to the controller
        while True and time.time() - tic < timeout:
            try:
                rc = parallel.Client(profile=self.profile_name)
                break
            except IOError:
                time.sleep(2)
        #wait for all engines to come online
        while True and time.time() - tic < timeout:
            if len(rc.ids) == self.nengines:
                return True
            else:
                time.sleep(2)
        return False
        
    def stop_cluster(self):
        cmd = ['ipcluster','stop','--profile='+self.profile_name]
        stop = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=os.setsid)
        time.sleep(1)

    def client(self):
        return parallel.Client(profile=self.profile_name)

    def delete_profile(self):
        if 'temp' not in self.profile_name:
            #we don't want to accidentally delete
            #profiles we've intended not to be temporary
            return False
        count = 0
        while True and count <20:
            try:
                count+=1
                shutil.rmtree(self.profile_dir)
                return True
            except OSError:
                time.sleep(1)
        return False

    def __del__(self):
        self.stop_cluster()
        if 'temp' in self.profile_name:
            self.delete_profile()
