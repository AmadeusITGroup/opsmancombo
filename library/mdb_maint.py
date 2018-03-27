#!/usr/bin/python

from ansible.module_utils.opsmanager import ansible_setup


if __name__ == '__main__':
    module, opsmanager = ansible_setup()

    group = opsmanager.get_group_by_name(module.params['cluster'])
    response = opsmanager.set_maintenance(group)
    module.exit_json(changed=False, meta=response)
