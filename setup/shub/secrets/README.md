Fill `Grid5000_creds.yaml` with your Grid5000 credentials.

Grid5000_creds.yaml

```yaml
username: <your_grid5000_username>
password: <your_grid5000_password>
```

The VPN connection files for Grid5000 are required. Please follow the guide to download your personal VPN files.
[Grid5000 VPN setup guide](https://www.grid5000.fr/w/VPN)

:exclamation: The VPN files, such as **.ovpn** **.key** and **.crt** must be extracted in this
directory (`dockerfile/secrets`) so that they can be mounted into the scalehub container.

:exclamation: An ssh private key for grid500 must be created (see
documentation [Grid5000 SSH setup guide](https://www.grid5000.fr/w/SSH#Generating_keys_for_use_with_Grid'5000)) and
copied to `dockerfile/secrets`.
Depending on how you name it, fix the secret filename field in `dockerfile/docker-compose.yaml`
