from service.filter_chain_service import load_line_checker_list


if __name__ == '__main__':
    project = 'vr_2'
    list_filter = load_line_checker_list(project=project)
    # list_filter.clean_temp_folder(project)
    list_filter.split_urls()
    print('finished')
