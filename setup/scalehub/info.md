The environment requires many secrets to be provided

> To properly setup the environment, you need to have the following:
> - A Grid5000 account (thus credentials)
> - A Grid5000 vpn config (.ovpn file)
> - A Grid5000 ssh key (to access the nodes)

> - Credentials are used by EnosLib to access the Grid5000 API and make the necessary calls to deploy the nodes.
> - The vpn config is used to connect to nodes directly via SSH.
> - The ssh key is used to access the nodes via SSH.
> 
> Other credentials (and configuration files) can be added to access other testbdes (e.g. Chameleon, FIT, custom testbeds, etc.).