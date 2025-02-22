## Python workitem agent example
This is a example project, functions as a template for how to get started writing a more controlable workitem agent for the OpenCore platform.

We connect to the server, and also register an event listener waiting for "SignedIn" events. 
When we get a "SignedIn" event, register a message queue based of the `queue` environment variable, and start listening for messages.
This should match the name of the workitem queue we want this agent to handle.

When we get a message, we will pop a workitem of the workitem queue, and if one is found ( in case more agents are listening )
we start processing it inside `__ProcessWorkitem` 
We then update the state of the workitem to successful or retry, depending on the outcome of `__ProcessWorkitem`.

When running inside an agent make sure the `wiq` environment variable has been set to the name of the workitem queue you want to listen to.
When running local, make sure to add this to your .env file.
If you need to use a different queue name from the workitem queue name, you can set the `queue` environment variable to something different.

```
https://github.com/openiap/pythonworkitemagent.git
```

# Build and run
Setup environment with micromamba and run using the following commands
```bash
micromamba create -y -n pythontest -f conda.yaml
# or
micromamba install -y -n pythontest -f conda.yaml
python cli.py 
```
To run using default python installation, use the following commands
```bash
pip uninstall openiap-edge
python -m pip cache purge
pip install openiap-edge==0.0.26
python cli.py 
```
github repository:

```
https://github.com/skadefro/pythontest.git 
```