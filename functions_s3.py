import boto3


class functions_s3():

    def __init__(self):

        pass


    @staticmethod
    def create_s3_bucket(bucket_name, region=None):

        """

        CREATE AN S3 BUCKET IN A SPECIFIED VERSION

        IF A REGION IS NOT SPECIFIED,
        THE BUCKET IS CREATED IN THE S3 DEFAULT
        REGION (us-east-1)

        # Arguments
            :param bucket_name: Bucket to create (String)
            :param region: String region to create
                           bucket in, e.g., 'us-west-2' (String)

        # Returns
            :return validator: True if bucket created,
                               else False (Boolean)

        """

        # INIT FUNCTION VALIDATOR
        validator = False

        try:
            if region is None:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)

            print("BUCKET CREATED SUCCESSFULLY")
            print("-"*50)

            validator = True

        except Exception as ex:
            print(ex)

        return validator


    @staticmethod
    def list_s3_existing_buckets():

        """

        GET ALL BUCKETS IN ACCOUNT.

        # Returns
            :return list_buckets: List of buckets (List)
        """

        # INIT VARIABLE THAT TO STORE THE LIST OF BUCKETS
        list_buckets = []

        try:
            # CONNECTING TO S3
            s3 = boto3.client('s3')

            # GETTING LIST OF BUCKETS
            response = s3.list_buckets()
            list_buckets = [bucket for bucket in response['Buckets']]

            print('LIST - BUCKETS')

            for bucket in list_buckets:
                print("BUCKET: {}".format(bucket["Name"]))

            print("-" * 50)

        except Exception as ex:
            print(ex)

        return list_buckets


    @staticmethod
    def upload_file_to_bucket_s3(filename, bucketname, file_after_upload):

        """

        UPLOAD FILES TO A BUCKET S3.

        # Arguments
            :param filename: File to upload (Path)
            :param bucketname: Bucket to use (String)
            :param file_after_upload: Name and
                   dir in the bucket, after upload (String)

        # Returns
            :return validator: True if upload was succesfully,
                               else False (Boolean)
        """

        # INIT FUNCTION VALIDATOR
        validator = False

        try:
            # CONNECTING TO S3
            s3 = boto3.client('s3')

            # UPLOAD FILE
            s3.upload_file(filename, bucketname, file_after_upload)

            print("UPLOADED SUCCESSFULLY")
            print("OBJECT: {}".format(filename))
            print("DESTINATION: s3://{}/{}".format(bucketname, file_after_upload))
            print("-" * 50)

            validator = True

        except Exception as ex:
            print(ex)

        return validator