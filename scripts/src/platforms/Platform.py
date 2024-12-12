from ansible.inventory.manager import InventoryManager


def merge_inventories(
    inv1: InventoryManager, inv2: InventoryManager
) -> InventoryManager:
    for group_name in inv2.groups:
        if group_name not in inv1.groups:
            inv1.add_group(group_name)
        for host in inv2.get_hosts(group_name):
            inv1.add_host(host.name, group=group_name)
    return inv1


class Platform:
    def setup(self) -> InventoryManager:
        raise NotImplementedError()

    def get_provider(self):
        raise NotImplementedError()

    def setup_single(self) -> InventoryManager:
        return self.setup()

    def setup_multi(self, roles, networks) -> InventoryManager:
        raise NotImplementedError()
