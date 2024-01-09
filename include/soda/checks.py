def check(scan_name, checks_subpath=None, data_source='ifs_table'):
    from soda.scan import Scan

    print(f'Soda scan on {data_source}...')
    
    base_path = 'include/soda'
    checks_path = base_path if checks_subpath is None else base_path + f'/{checks_subpath}'

    scan = Scan()
    scan.add_configuration_yaml_file(f"{base_path}/configuration.yml")
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()

    if result != 0:
        raise ValueError('Scan failed!')

    return result