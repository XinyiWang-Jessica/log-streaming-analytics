
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern1 = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
ts_pattern2 = r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'


month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

week_map = {
  1: 'Sunday', 2: 'Monday', 3 : 'Tuesday', 4 : 'Wednesday', 5 : 'Thursday', 6 : 'Friday', 7 : 'Saturday'
}