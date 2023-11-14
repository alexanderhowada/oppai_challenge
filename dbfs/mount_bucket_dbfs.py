# Databricks notebook source
from utils.logs import print_args

# COMMAND ----------

# MAGIC %run ./constants_dbfs

# COMMAND ----------

@print_args(print_kwargs=['mount_path', 'mount_name'])
def mount_dbfs(mount_path: str, mount_name: str, mount_on='/mnt'):
    """Mount S3 in Databricks as a file system.

    Args:
        mount_path: s3 path to mount.
        mount_name: target folder to mount.
        mount_on: default path to mount.

    Returns:
        dbutils.fs.ls(mount_name)
    """
    mount_name = mount_on.rstrip('/')+'/'+mount_name
    dbutils.fs.mount(mount_path, mount_name)

    return dbutils.fs.ls(mount_name)

@print_args(print_kwargs=['mount_name'])
def umount_dbfs(mount_name: str, mount_on='/mnt'):
    """Umount bucket.

    Args:
        mount_name: name of the folder that the bucket is mounted at.
        mount_on: root folder. Defaults to '/mnt'
    """
    mount_name = mount_on.rstrip('/')+'/'+mount_name
    dbutils.fs.unmount(mount_name)

# COMMAND ----------

print("Mounting.")
for config in TO_MOUNT:
    # continue
    mount_dbfs(**config)

print("Unmounting.")
for config in TO_UNMOUNT:
    umount_dbfs(**config)

# COMMAND ----------



# COMMAND ----------

!ls /dbfs/mnt/oppai/anexos
