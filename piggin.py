# -*- coding: utf-8 -*-
"""
Created on Mon Aug 06 09:48:07 2018
to do: add 'sync', bulk upload and downloads
@author: Prodipta
"""
import boto3
import argparse
import sys
import os

class AwsS3Py(object):
    
    def __init__(self, config=None):
        self._s3r = boto3.resource('s3')
        self._s3c = boto3.client('s3')
        if config:
            self.config(config['access_key'], config['secret_key'])
    
    def config(self, access_key=None, secret_key=None):
        os.environ['AWS_ACCESS_KEY_ID']=access_key
        os.environ['AWS_SECRET_ACCESS_KEY']=secret_key
    
    def mkbucket(self, bucket_name, acl, location):
        self.create_bucket(bucket_name, acl=acl, location=location)                
            
    def mkdir(self, strpath):
        protocol, bucket, key, path = self.parse_path(strpath)
        if protocol == 's3':
            if not key.endswith('/'):
                key = key+'/'
            self.touch(bucket, key)
    
    def copy(self, str_src, str_dest):
        protocol1, bucket1, key1, path1 = self.parse_path(str_src)
        protocol2, bucket2, key2, path2 = self.parse_path(str_dest)
        
        if protocol1 == 's3' and protocol2 == 'file':
            self.download(bucket1, key1, path2)
        elif protocol1 == 'file' and protocol2 == 's3':
            self.upload(bucket2,key2,path1)
        else:
            raise('Unknown source {} or destination {}'.format(str_src, str_dest))
    
    def rm(self, str_path):
        protocol, bucket, key, path = self.parse_path(str_path)
        if protocol == 's3':
            if key == '':
                raise('Attempt to delete a bucket, call delete')
            else:
                self.delete_object(bucket, key)
        else:
            raise('Path {} not an s3 object'.format(str_path))
    
    def delete(self, str_path):
        protocol, bucket, key, path = self.parse_path(str_path)
        if protocol == 's3':
            if key == '':
                self.delete_bucket(bucket)
            else:
                raise('Path {} not an s3 bucket'.format(str_path))
        else:
            self.delete_bucket(str_path)
    
    def ls(self, str_path):
        protocol, bucket, key, path = self.parse_path(str_path)
        if protocol == 's3':
            return self.list_objects(bucket, key)
        else:
            return self.list_objects(str_path, '')
            
    def lsbucket(self):
        try:
            response = self._s3c.list_buckets()
            buckets = [b['Name'] for b in response['Buckets']]
            return buckets
        except:
            raise
            
    def list_objects(self, bucket_name, key=''):
        items = []
        objects = list_s3_objects(self._s3c, bucket_name, key)
        for o in objects:
            items.append(o)
        return items
    
    def upload(self, bucket_name, key, file_name):
        with open(file_name, 'rb') as fd:
            try:
                self._s3r.Object(bucket_name, key).put(Body=fd)
            except:
                raise
                
    def touch(self, bucket_name, key):
        try:
            self._s3r.Object(bucket_name, key).put(Body='')
        except:
            raise
    
    def download(self, bucket_name, key, file_name):
        try:
            self._s3r.Bucket(bucket_name).download_file(key, file_name)
        except:
            raise
    
    def delete_object(self, bucket_name, key):
        try:
            obj = self._s3r.Object(bucket_name, key)
            obj.delete()
        except:
            raise
    
    def create_bucket(self, bucket_name, *args, **kwargs ):
        location = kwargs.pop('location', 'us-west-1')
        acl = kwargs.pop('acl', 'private')
        
        if not location:
            location = 'us-west-1'
        if not acl:
            acl = 'private'
        
        acl = acl.strip('\"').strip("\'")
        location = location.strip('\"').strip("\'")
        
        try:
            self._s3r.create_bucket(Bucket=bucket_name, ACL = acl,
            CreateBucketConfiguration={'LocationConstraint':location})
        except:
            raise
            
    def delete_bucket(self, bucket_name):
        try:
            bucket = self._s3r.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except:
            raise
            
    def parse_path(self, str_path):
        protocol = str_path.split(':::')

        if len(protocol) == 2:
            if protocol[0].lower() == 's3':
                bucket = protocol[1].split('/')[0]
                key = protocol[1][len(bucket)+1:]
                return 's3', bucket, key, None
            else:
                raise('Unknown protocol in path: {}!'.format(protocol[0]))
            
        elif len(protocol) == 1:
            path = protocol[0]
            return 'file', None, None, path
        else:
            raise('Illegal path name: {}!'.format(str_path))
        
def list_s3_objects(client, bucket_name, key):
    results = client.list_objects(Bucket=bucket_name,Prefix=key, Delimiter='/')
    for r in results.get('Contents',[]):
        yield(r.get('Key'))
        
    for r in results.get('CommonPrefixes', []):
        yield(r.get('Prefix'))
    
def msg():
    return '''
       available commands
         [lsbucket: list all existing buckets on S3 ]
         [mkbucket <bucket>: create a new bucket]
         [delete <bucket>: delete an existing bucket]
         [ls <bucket or path>: list all objects in the bucket ]
         [mkdir path: create a new `directory` in a bucket]
         [copy src dest: copy between local file system and S3]
         [rm path: delete an object in the bucket]
       NOTE: S3 path should starts with s3::: (example s3:::bucket/path/to/file)  
        '''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage=msg())
    parser.add_argument("command",nargs='*',help="run the command options")
    parser.add_argument('-c','--config',nargs='*', help="specify configuration file")
    parser.add_argument('-acl','--create-bucket-acl',nargs='*', help="specify bucket access control list")
    parser.add_argument('-cbc','--create-bucket-configuration',nargs='*', help="specify bucket creation configuration")
    args = parser.parse_args()
    
    if len(args.command) < 1:
        print('must specify a command.')
        parser.print_help()
        sys.exit(1)
    
    config=None
    acl=None
    bucket_config=None
    s3util = AwsS3Py()
    exit()
    
    if args.config:
        if len(args.config) == 2:
            s3util.config(access_key=args.config[0],
                          secret_key=args.config[1])
        else:
            print('config option must specify access and secret keys.')
            parser.print_help()
            sys.exit(1)
        config = args.config[0]
        
    if args.create_bucket_acl:
        if len(args.create_bucket_acl) <> 1:
            print('acl option must specify acl list.')
            parser.print_help()
            sys.exit(1)
        acl = args.create_bucket_acl[0]
        
    if args.create_bucket_configuration:
        if len(args.create_bucket_configuration) <> 1:
            print('bucket config option must specify a dict.')
            parser.print_help()
            sys.exit(1)
        bucket_config = args.create_bucket_configuration[0]
    
    if args.command[0] == 'ls':
        if len(args.command) <> 2:
            print('ls command must specify a bucket name.')
            parser.print_help()
            sys.exit(1)
        
        objects = s3util.ls(args.command[1])
        for o in objects:
            print(o)
            
    elif args.command[0] == 'lsbucket':
        buckets = s3util.lsbucket()
        for b in buckets:
            print(b)
    
    elif args.command[0] == 'mkbucket':
        if len(args.command) <> 2:
            print('mkbucket command must specify a bucket name.')
            parser.print_help()
            sys.exit(1)
            
        buckets = s3util.mkbucket(args.command[1], acl=acl,location=bucket_config )
    
    elif args.command[0] == 'delete':
        if len(args.command) <> 2:
            print('delete command must specify a bucket name.')
            parser.print_help()
            sys.exit(1)
            
        buckets = s3util.delete(args.command[1])
    
    elif args.command[0] == 'mkdir':
        if len(args.command) <> 2:
            print('mkdir command must specify a directory name.')
            parser.print_help()
            sys.exit(1)
            
        buckets = s3util.mkdir(args.command[1])
        
    elif args.command[0] == 'rm':
        if len(args.command) <> 2:
            print('rm command must specify an S3 object path.')
            parser.print_help()
            sys.exit(1)
            
        buckets = s3util.rm(args.command[1])
        
    elif args.command[0] == 'copy':
        if len(args.command) <> 3:
            print('copy command must specify a source and a destination.')
            parser.print_help()
            sys.exit(1)
            
        buckets = s3util.copy(args.command[1],args.command[2])