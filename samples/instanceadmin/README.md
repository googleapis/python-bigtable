[//]: # "This README.md file is auto-generated, all changes to this file will be lost."
[//]: # "To regenerate it, use `python -m synthtool`."

## Python Samples for Cloud Bigtable

This directory contains samples for Cloud Bigtable, which may be used as a refererence for how to use Cloud Bigtable. 
Samples, quickstarts, and other documentation are available at <a href="https://cloud.google.com/bigtable">cloud.google.com</a>.


### cbt Command Demonstration

This page explains how to use the cbt command to connect to a Cloud Bigtable instance, perform basic administrative tasks, and read and write data in a table. More information about this quickstart is available at https://cloud.google.com/bigtable/docs/quickstart-cbt


<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/python-bigtable&page=editor&open_in_editor=instanceadmin.py"><img alt="Open in Cloud Shell" src="http://gstatic.com/cloudssh/images/open-btn.png"> 
</a>

To run this sample:

1. If this is your first time working with GCP products, you will need to set up [the Cloud SDK][cloud_sdk] or utilize [Google Cloud Shell][gcloud_shell]. This sample may [require authetication][authentication].

1. Make a fork of this repo and clone the branch locally, then navigate to the sample directory you want to use.

1. Install the dependencies needed to run the samples.

        pip install -r requirements.txt

1. Run the sample using

        python instanceadmin.py


`usage: instanceadmin.py [-h] [run] [dev-instance] [del-instance] [add-cluster] [del-cluster] project_id instance_id cluster_id`

## Additional Information

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
