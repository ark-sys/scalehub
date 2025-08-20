"""Mock data fixtures for testing."""

from typing import Dict, Any, List
from unittest.mock import MagicMock


class MockData:
    """Mock data for platform testing."""

    @staticmethod
    def raspberry_pi_inventory() -> Dict[str, Any]:
        """Mock Raspberry Pi inventory data."""
        return {
            "pico": {
                "hosts": {
                    "pi1": {
                        "ansible_ssh_host": "192.168.1.1",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    },
                    "pi2": {
                        "ansible_ssh_host": "192.168.1.2",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    },
                    "pi3": {
                        "ansible_ssh_host": "192.168.1.3",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    },
                    "pi4": {
                        "ansible_ssh_host": "192.168.1.4",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    },
                    "pi5": {
                        "ansible_ssh_host": "192.168.1.5",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    },
                    "pi6": {
                        "ansible_ssh_host": "192.168.1.6",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa"
                    }
                }
            }
        }

    @staticmethod
    def enos_inventory() -> Dict[str, Any]:
        """Mock Enos inventory data."""
        return {
            "G5k": {
                "hosts": {
                    "paradoxe-1.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.1",
                        "ansible_user": "root",
                        "ipv6_alias": "paradoxe-1-ipv6.rennes.grid5000.fr"
                    },
                    "paradoxe-2.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.2",
                        "ansible_user": "root",
                        "ipv6_alias": "paradoxe-2-ipv6.rennes.grid5000.fr"
                    }
                }
            },
            "control": {
                "hosts": {
                    "paradoxe-1.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.1",
                        "ansible_user": "root",
                        "cluster_role": "control"
                    }
                }
            },
            "agents": {
                "hosts": {
                    "paradoxe-2.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.2",
                        "ansible_user": "root",
                        "cluster_role": "consumer"
                    }
                }
            }
        }

    @staticmethod
    def vagrant_inventory() -> Dict[str, Any]:
        """Mock Vagrant inventory data."""
        return {
            "vagrant": {
                "hosts": {
                    "paradoxe-1.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.1",
                        "ansible_user": "root",
                        "reservation_name": "vagrant_platform"
                    },
                    "paradoxe-2.rennes.grid5000.fr": {
                        "ansible_host": "172.16.64.2",
                        "ansible_user": "root",
                        "reservation_name": "vagrant_platform"
                    }
                }
            }
        }

    @staticmethod
    def vm_groups() -> List[Dict[str, Any]]:
        """Mock VM groups for Vagrant platforms."""
        return [
            {
                "role": "producers",
                "conf": {
                    "core_per_vm": 4,
                    "memory_per_vm": 8192,
                    "disk_per_vm": 100,
                    "site": "rennes",
                    "cluster": "paradoxe"
                },
                "count": 2,
                "required_nodes": 1
            },
            {
                "role": "consumers",
                "conf": {
                    "core_per_vm": 2,
                    "memory_per_vm": 4096,
                    "disk_per_vm": 50,
                    "site": "rennes",
                    "cluster": "paradoxe"
                },
                "count": 1,
                "required_nodes": 1
            }
        ]

    @staticmethod
    def grid5000_api_nodes_response() -> Dict[str, Any]:
        """Mock Grid5000 API response for nodes."""
        return {
            "items": [
                {
                    "uid": "paradoxe-1",
                    "architecture": {
                        "nb_cores": 52,
                        "nb_procs": 2,
                        "nb_threads": 104
                    },
                    "main_memory": {
                        "ram_size": 412316860416  # 384 GB in bytes
                    },
                    "network_adapters": [
                        {
                            "interface": "Ethernet",
                            "rate": 25000000000
                        }
                    ]
                }
            ]
        }

    @staticmethod
    def mock_enos_roles() -> Dict[str, List[MagicMock]]:
        """Mock Enos roles with host objects."""
        host1 = MagicMock()
        host1.alias = "paradoxe-1.rennes.grid5000.fr"
        host1.address = "172.16.64.1"

        host2 = MagicMock()
        host2.alias = "paradoxe-2.rennes.grid5000.fr"
        host2.address = "172.16.64.2"

        return {
            "G5k": [host1, host2],
            "control": [host1],
            "producers": [host1],
            "consumers": [host2]
        }

    @staticmethod
    def mock_vm_roles() -> Dict[str, List[MagicMock]]:
        """Mock VM roles with host objects."""
        vm1 = MagicMock()
        vm1.alias = "vm-1"
        vm1.address = "10.0.0.1"
        vm1.pm = MagicMock()
        vm1.pm.alias = "paradoxe-1.rennes.grid5000.fr"
        vm1.pm.address = "172.16.64.1"

        vm2 = MagicMock()
        vm2.alias = "vm-2"
        vm2.address = "10.0.0.2"
        vm2.pm = MagicMock()
        vm2.pm.alias = "paradoxe-2.rennes.grid5000.fr"
        vm2.pm.address = "172.16.64.2"

        return {
            "VMonG5k": [vm1, vm2],
            "G5k": []
        }

    @staticmethod
    def expected_raspberry_pi_inventory() -> Dict[str, Any]:
        """Expected Raspberry Pi inventory after setup."""
        return {
            "producers": {
                "hosts": {
                    "pi1": {
                        "ansible_ssh_host": "192.168.1.1",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    },
                    "pi2": {
                        "ansible_ssh_host": "192.168.1.2",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    }
                }
            },
            "consumers": {
                "hosts": {
                    "pi3": {
                        "ansible_ssh_host": "192.168.1.3",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi4": {
                        "ansible_ssh_host": "192.168.1.4",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi5": {
                        "ansible_ssh_host": "192.168.1.5",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    }
                }
            },
            "pico": {
                "hosts": {
                    "pi1": {
                        "ansible_ssh_host": "192.168.1.1",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    },
                    "pi2": {
                        "ansible_ssh_host": "192.168.1.2",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    },
                    "pi3": {
                        "ansible_ssh_host": "192.168.1.3",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi4": {
                        "ansible_ssh_host": "192.168.1.4",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi5": {
                        "ansible_ssh_host": "192.168.1.5",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    }
                }
            },
            "agents": {
                "hosts": {
                    "pi1": {
                        "ansible_ssh_host": "192.168.1.1",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    },
                    "pi2": {
                        "ansible_ssh_host": "192.168.1.2",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "producer"
                    },
                    "pi3": {
                        "ansible_ssh_host": "192.168.1.3",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi4": {
                        "ansible_ssh_host": "192.168.1.4",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    },
                    "pi5": {
                        "ansible_ssh_host": "192.168.1.5",
                        "ansible_user": "pi",
                        "ansible_ssh_private_key_file": "~/.ssh/id_rsa",
                        "cluster_role": "consumer"
                    }
                }
            }
        }