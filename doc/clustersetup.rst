Hadoop Cluster Setup
====================

Supported Versions
------------------
Any version of Hadoop that has built in TypedBytes support (Apache 0.21+, CDH2+) should work with all features (if not please submit a bug report).  The version that currently has the most testing is CDH3, and CDH2/4 have also been tested.  Hadoopy can be used with text input/output (see the :doc:`text guide guide</text>`) in all Hadoop distributions that support streaming; however, this input/output method is not recommended as TypedBytes handles serialization, is unambiguous (i.e., no need to worry about binary data with tabs or new lines), and is more efficient.

launch vs. launch_frozen
------------------------
An important consideration is whether you want to use launch or launch_frozen primarily to start jobs.  The preferred method is launch as there is very little startup overhead; however, it requires each node of the cluster to have Python, Hadoopy, and all libraries that you use in your job.  The benefit of launch_frozen is that you can use it on clusters that don't even have Python, that you have no administrative access to, or don't want to have to maintain (e.g., ephemeral EC2 clusters to run a job).  If you want to use launch, then be prepared to have some method for keeping the cluster homogeneous and up-to-date (e.g., using puppet_ or fabric_).  If you use launch_frozen, then you have a 15+ second penalty each time PyInstaller freezes your script which is mitigated in large workflows as repeated called to launch_frozen for the same script only use PyInstaller once by default (overrided by setting cache=False).  This can be further mitagated by manually maintaining PyInstaller paths on HDFS and setting the frozen_tar_path argument (useful if the jobs are not modified often).  Ideally if the PyInstaller time can be brought down to a second or a more aggressive caching stategy is used, launch_frozen would be the preferred method in nearly all cases (we are currently working on this).

.. TODO Give an example of this in Hadoopy Helper


.. _puppet: http://puppetlabs.com
.. _fabric: http://fabfile.org

Whirr
-----
The easiest way to install Hadoop on EC2 is to use whirr_.

.. _whirr: http://whirr.apache.org

.. raw:: html

    <script src="https://gist.github.com/3195358.js?file=hadoop.properties"></script>
