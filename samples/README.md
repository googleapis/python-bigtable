[//]: # "This README.md file is auto-generated, all changes to this file will be lost."
[//]: # "To regenerate it, use `python -m synthtool`."

## Python Samples for Cloud Bigtable

This directory contains samples for Cloud Bigtable, which may be used as a refererence for how to use Cloud Bigtable. 
Samples, quickstarts, and other documentation are available at <a href="https://cloud.google.com/bigtable">cloud.google.com</a>.


### Beam

This sample demonstrates writing data to Bigtable with Beam.



### Hello World in Cloud Bigtable

Demonstrates how to connect to Cloud Bigtable and run some basic operations. More information available at: https://cloud.google.com/bigtable/docs/samples-python-hello


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=main.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python main.py


`usage: main.py [-h] [--table TABLE] project_id instance_id`

### Hello World using HappyBase

This sample demonstrates using the Google Cloud Client Library HappyBase package, an implementation of the HappyBase API to connect to and interact with Cloud Bigtable. More information available at: https://cloud.google.com/bigtable/docs/samples-python-hello-happybase


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=main.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python main.py


`usage: main.py [-h] [--table TABLE] project_id instance_id`

### cbt Command Demonstration

This page explains how to use the cbt command to connect to a Cloud Bigtable instance, perform basic administrative tasks, and read and write data in a table. More information about this quickstart is available at https://cloud.google.com/bigtable/docs/quickstart-cbt


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=instanceadmin.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python instanceadmin.py


`usage: instanceadmin.py [-h] [run] [dev-instance] [del-instance] [add-cluster] [del-cluster] project_id instance_id cluster_id`

### Metric Scaler

This sample demonstrates how to use Stackdriver Monitoring to scale Cloud Bigtable based on CPU usage.


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=metricscaler.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python metricscaler.py


`usage: metricscaler.py [-h] [--high_cpu_threshold HIGH_CPU_THRESHOLD] [--low_cpu_threshold LOW_CPU_THRESHOLD] [--short_sleep SHORT_SLEEP] [--long_sleep LONG_SLEEP] bigtable_instance bigtable_cluster`

### Quickstart

Demonstrates of Cloud Bigtable. This sample creates a Bigtable client, connects to an instance and then to a table, then closes the connection.


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=main.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python main.py


`usage: main.py [-h] [--table TABLE] project_id instance_id`

### Quickstart using HappyBase

Demonstrates of Cloud Bigtable using HappyBase. This sample creates a Bigtable client, connects to an instance and then to a table, then closes the connection.


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=main.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python main.py


`usage: main.py [-h] [--table TABLE] project_id instance_id`

### Snippets

This folder contains snippets for Python Cloud Bigtable.



### Table Admin

Demonstrates how to connect to Cloud Bigtable and run some basic operations.


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=tableadmin.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication] and you will need to [enable billing][enable_billing].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python tableadmin.py


`usage: tableadmin.py [-h] [run] [delete] [--table TABLE] project_id instance_id`

## Additional Information

These samples use the [Google Cloud Client Library for Python][client_library_python].
You can read the documentation for more details on API usage and use GitHub
to [browse the source][source] and [report issues][issues].

### Contributing
For [contributing guidelines][contrib_guide], the [Python style guide][py_style], and more information on prerequisite steps to contribute, view the source code at <a href="https://github.com/googleapis/python-bigtable">googleapis/python-bigtable</a>.

[authentication]: https://cloud.google.com/docs/authentication/getting-started
[enable_billing]:https://cloud.google.com/apis/docs/getting-started#enabling_billing
[client_library_python]: https://googlecloudplatform.github.io/google-cloud-python/
[source]: https://github.com/GoogleCloudPlatform/google-cloud-python
[issues]: https://github.com/GoogleCloudPlatform/google-cloud-python/issues
[contrib_guide]: https://github.com/googleapis/google-cloud-python/blob/master/CONTRIBUTING.rst
[py_style]: http://google.github.io/styleguide/pyguide.html
[cloud_sdk]: https://cloud.google.com/sdk/docs
[gcloud_shell]: https://cloud.google.com/shell/docs
[gcloud_shell]: https://cloud.google.com/shell/docs
