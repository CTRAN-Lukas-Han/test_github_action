import fabric
from dagster import ConfigurableResource, get_dagster_logger

class SFTPResource(ConfigurableResource):
    user: str
    host: str
    keyfile: str
    keypass: str

    def listdir(self):
        c = fabric.Connection(host=self.host, user=self.user, 
            connect_kwargs={"key_filename":self.keyfile, "password":self.keypass })
        sftp=c.sftp()
        l=sftp.listdir()
        c.close()
        return l

    def uploadFile(self,filepath,filename):
        c = fabric.Connection(host=self.host, user=self.user, 
            connect_kwargs={"key_filename":self.keyfile, "password":self.keypass })
        sftp=c.sftp()
        stat=sftp.put(filepath, "." + filename, confirm=True)
        c.close()
        return stat


class StubSFTP:
    def listdir(self):
        logger = get_dagster_logger()
        logger.info("Stub SFTP executing listdir")
        return []

    def uploadFile(self,filepath,filename):
        logger = get_dagster_logger()
        logger.info("Stub SFTP executing uploadFile")
        return None
