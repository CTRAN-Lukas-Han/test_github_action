import os
import subprocess
from dagster import ConfigurableResource

dataloader_loc = os.path.join(os.getenv("BI_DIRECTORY_BASE"),"Departments/Maintenance/Trapeze Dataloader 22.0.2")
dataloader_exe = os.path.join(dataloader_loc, "FADataLoader.exe")

Class DATALOADER_Resource(ConfigurableResource):
    host: str
    port: int
    user: str
    password: str

    def load_file(self.host,self.port,filepath):
        subprocess.run([
            "mono",dataloader_exe,
            "-n", "1",
            "-i",filepath,
            "-l",log_loc,
            "-a",self.host+":"+self.port,
            "-u",self.user,
            "-p",self.password])
