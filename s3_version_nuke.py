"""
 inputs:
 1. Profile name (or) Access key and Secret Key
 2. Bucket name
 3. Prefix (optional)
 Calculate the size and count of the total number of delete markers, current and non current objects. Will ask for a
 prompt to delete the delete markers, current and non-current objects.
"""
import sys
from boto3 import client, Session
from botocore.exceptions import ProfileNotFound, ClientError


def calculate_size(size, _size_table):
    count = 0
    while size // 1024 > 0:
        size = size / 1024
        count += 1
    return str(round(size, 2)) + ' ' + _size_table[count]


def get_credentials():
    credentials_verified = False
    aws_access_key_id = None
    aws_secret_access_key = None
    while not credentials_verified:
        ch = input("> Press 1 to use an existing profile\n"
                   "> Press 2 to enter an Access Key/Secret Key\n"
                   "> Press 3 to exit\n"
                   "> ")
        if ch.strip() == "1":
            aws_access_key_id, aws_secret_access_key = select_profile()
            if aws_access_key_id is not None and aws_secret_access_key is not None:
                credentials_verified = True
        elif ch.strip() == "2":
            aws_access_key_id = input("> AWS access key").strip()
            aws_secret_access_key = input("> AWS secret access key").strip()
            credentials_verified = True
        elif ch.strip() == "3":
            sys.exit(0)
        else:
            print("Invalid option, please try again")
    return aws_access_key_id, aws_secret_access_key


def select_profile():
    profile_selected = False
    while not profile_selected:
        try:
            profiles = Session().available_profiles
            if len(profiles) == 0:
                return None, None
            print("> Available Profiles: ", profiles)
        except Exception as e:
            print(e)
            return None, None
        profile_name = input("> Profile name: ").strip().lower()
        try:
            session = Session(profile_name=profile_name)
            credentials = session.get_credentials()
            aws_access_key_id = credentials.access_key
            aws_secret_access_key = credentials.secret_key
            profile_selected = True
            return aws_access_key_id, aws_secret_access_key
        except ProfileNotFound:
            print("> Invalid profile")
        except Exception as e:
            raise e


def create_connection_and_test(aws_access_key_id: str, aws_secret_access_key: str, _bucket):
   
    try:
        _s3_client = client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

        _s3_client.list_buckets()

        try:
            _s3_client.head_bucket(Bucket=bucket)
        except ClientError:
        
            raise Exception("> bucket doesn't exist in the account, re-check the name/spelling ")

        return _s3_client

    except ClientError:
        print("Invalid Access/Secret keys")
    except Exception as e:
        raise e
    return None


if __name__ == '__main__':

    size_table = {0: 'Bs', 1: 'KBs', 2: 'MBs', 3: 'GBs', 4: 'TBs', 5: 'PBs', 6: 'EBs'}

    print("\n")
    print("\n")
    print("> starting script...")

    access_key_id, secret_access_key = get_credentials()

    bucket = input("> Enter the name of the bucket: ").strip()

    prefix = input("> Enter a prefix (leave blank if you don't need one)").strip()

    s3_client = create_connection_and_test(access_key_id, secret_access_key, bucket)

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    if len(prefix) > 0:
        operation_parameters = {'Bucket': bucket,
                                'Prefix': prefix}
    else:
        operation_parameters = {'Bucket': bucket}


    delete_marker_count = 0
    delete_marker_size = 0
    versioned_object_count = 0
    versioned_object_size = 0
    current_object_count = 0
    current_object_size = 0
    delete_list = []

    print("> Calculating... may take a while")
    for object_response_itr in object_response_paginator.paginate(**operation_parameters):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                delete_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})
                delete_marker_count += 1

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                delete_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
                if version['IsLatest'] is False:
                    versioned_object_count += 1
                    versioned_object_size += version['Size']

                elif version['IsLatest'] is True:
                    current_object_count += 1
                    current_object_size += version['Size']

    print("\n")
    print("-" * 10)
    print("> Total Delete markers: " + str(delete_marker_count))
    print("> Number of Current objects: " + str(current_object_count))
    print("> Current Objects size: ", calculate_size(current_object_size, size_table))
    print("> Number of Non-current objects: " + str(versioned_object_count))
    print("> Non-current Objects size: ", calculate_size(versioned_object_size, size_table))
    print("> Total size of current + non current objects: ",
          calculate_size(versioned_object_size + current_object_size, size_table))
    print("-" * 10)
    print("\n")

    delete_flag = False
    while not delete_flag:
        choice = input("> Do you wish to delete the delete markers, current, and non-current objects? [y/n] ")
        lock = input("> Add Governance Mode override (requires:root/admin/s3:BypassGovernanceRetention privileges)? [y/n] ")
        if choice.strip().lower() == 'y' and lock.strip().lower() == 'y':
            delete_flag = True
            print("> starting deletion...")
            print("> removing delete markers, current and non current 1000 at a time")
            for i in range(0, len(delete_list), 1000):
                response = s3_client.delete_objects(
                    Bucket=bucket,
                    BypassGovernanceRetention=True,
                    Delete={
                        'Objects': delete_list[i:i + 1000],
                        'Quiet': True
                    }
                )
                print(response)

        elif choice == 'y' and lock == 'n':
            delete_flag = True
            print("> starting deletion...")
            print("> removing delete markers, current and non current 1000 at a time")
            for i in range(0, len(delete_list), 1000):
                response = s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={
                        'Objects': delete_list[i:i + 1000],
                        'Quiet': True
                    }
                )
                print(response)

        elif choice == 'n' and lock == 'n':
            delete_flag = True

        else:
            print("> invalid choice please try again.")

    print("> All deletions successful")
    print("\n")
    print("\n")
