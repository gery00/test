#!/usr/bin/env php
<?php

define('S_SYSTEM_ROOT', dirname(__DIR__));
define('S_PROJECT_NAME', 'ircs_collect');
define('S_CONFIG_PATH', S_SYSTEM_ROOT . '/etc/');
define('S_VERSION_MAJOR', 2);
define('S_VERSION_MINOR', 0);
define('S_PID_FILE', S_SYSTEM_ROOT . '/var/run/' . S_PROJECT_NAME . '.pid');
define('S_OPT_FILE', S_SYSTEM_ROOT . '/var/opt/' . S_PROJECT_NAME . '_opt' . '.txt');

$option_list[] = array('version', 'v', 0);
$option_list[] = array('help', 'h', 0);
$option_list[] = array('config', 'c', 1);
$options = options_init($option_list);

$url = "http://ircs.p5w.net/ircs/interaction/queryQuestionByGszz.do";
$method = "post";

//正则表达式匹配规则
//总的页数
$rule['pages'] = '/共(\d+)页/is';
$rule['list'] = '/<table.*?class=\"req_box2\".*?>(.*?)<\/table>/is';
$rule['mktseccode'] = '/<td width="110" class="hd_td">.*?<br \/>\s*(\d+)\s*<\/td>/is';
$rule['question_id'] = '/<a .*?questionId=(\d+)&.*?">/is';
$rule['stockname'] = '/<td width="110" class="hd_td">(.*?)<br \/>/is';
$rule['questioner'] = '/<td width="90" class="hd_td".*?>(.*?)<\/td>/is';
$rule['respondent'] = '/<td width="90" class="hd_td2".*?>(.*?)<\/td>/is';
$rule['question_time'] = '/<td width="65" class="hd_td">([\d\s-:]+)<\/td>/is';
$rule['answer_time'] = '/<td width="65" class="hd_td2">([\d\s-:]+)<\/td>/is';
$rule['question'] = '/<td width="360" class="hd_td1".*?><a.*?>\s*(.*?)\s*<\/a> <\/td>/is';
$rule['answer'] = '/<td width="360" class="hd_td3".*?>(.*?)\s*<font.*?<\/td>/is';
$rule['answerflag'] = '/<td width="60" class="hd_td">(.*?)<\/td>/is';

//post数据, 查询条件
$conditions['searchType'] = 'name'; //name: 提问人，code: 公司代码，content：内容
$conditions['status'] = -1; //-1：全部，3：已回复，1：未回复
$conditions['searchRangeRadio'] = 0; //0:最近一年内， -1：历史数据
$conditions['searchRange'] = 0;
$conditions['keyWord'] = '';	//搜索关键字
$pageNo = '1';

$config_file = S_CONFIG_PATH . S_PROJECT_NAME . '.conf';
if (isset($options['help'])){
	display_usage();
	exit();
}else if (isset($options['version'])){
	display_version();
	exit();
}else if(isset($options['config'])){
	$config_file = S_CONFIG_PATH . $options['config'];
}

$command = command_init();
$configs = configs_init($config_file);

main();

function main(){
	global $command;


	switch($command){
		case 'build':
			data_build();
			break;
		case 'update':
			data_update();
			break;
		case 'clean':
			data_clean();
			break;
		case 'help':
			display_usage();
			break;
		default:
			echo "Unknow Command!\n";
	}

}

//对历史数据的采集
function data_build(){
	pid_lock();

	global $url;
	global $rule;
	global $conditions;
	global $configs;
	echo "[Data Build]历史数据采集\n";

	$date = input_date($configs);
	if($date == -1){
		exit();
	}
	$date_from = $date['date_f'];
	$date_to = $date['date_t'];

	$pdo = pdo_connect($configs);

	while(strtotime($date_from) <= strtotime($date_to)){
		$ret = data_collect($date_from, $url, $pdo, $rule, $conditions);
		if(!$ret){
			break;
		}
		$date_from = date_increment($date_from);
	}

	pdo_close($pdo);

	pid_unlock();
}

//实时数据的采集
function data_update(){
	echo "*********************Update Begin**********************\n";
	pid_lock();

	global $url;
	global $rule;
	global $conditions;
	global $configs;
	echo '[' . date("Y-m-d H:i:s", time()) . ']' . "[Data Update]实时数据采集\n";

	$pdo = pdo_connect($configs);

	//判断记录最后更新的时间文件是否存在，不存在则创建，从数据库中获取最后时间并写入文件
	if(!file_exists(S_OPT_FILE)){
		echo "OPT file not exists\n";
		my_mkdir(dirname(S_OPT_FILE));
		$sql_atime = "SELECT answer_time FROM ircs_main WHERE answerflag=1 ORDER BY answer_time DESC LIMIT 1";
		$sql_qtime = "SELECT question_time FROM ircs_main WHERE answerflag=0 ORDER BY question_time DESC LIMIT 1";
		if($ret = $pdo->query($sql_atime)){
			$row = $ret->fetch(PDO::FETCH_ASSOC);
			$atime = $row['answer_time'];
		}

		if($ret = $pdo->query($sql_qtime)){
			$row = $ret->fetch(PDO::FETCH_ASSOC);
			$qtime = $row['question_time'];
		}

		if($fp = fopen(S_OPT_FILE, 'w')){
			$buffer = "answer_time=$atime\nquestion_time=$qtime\n";
			fputs($fp, $buffer);
			fclose($fp);
		}
	}

	//读取文件中最后答复时间和未回复的最后提问时间
	$atime_last = kv_get(S_OPT_FILE, 'answer_time');
	$qtime_last = kv_get(S_OPT_FILE, 'question_time');

	//未回复数据的实时采集
	$date = date("Y-m-d", time());
	unanswered_collect($date, $url, $qtime_last, $rule, $pdo);

	//已回复数据实时采集
	answered_collect($date, $url, $atime_last, $rule, $pdo);

	pdo_close($pdo);
	pid_unlock();
	echo "*********************Update End************************\n";
}

//数据清除
function data_clean(){
	pid_lock();
	global $configs;
	echo "[Data Clean]清理数据\n";
	
	$pdo = pdo_connect($configs);

	$date = input_date($configs);
	if($date == -1){
		exit();
	}
	
	$date = quote($date, $pdo);
	$date_from = $date['date_f'];
	$date_to = $date['date_t'];

	$sql = "DELETE FROM ircs_main WHERE question_time BETWEEN {$date_from} AND {$date_to}";

	$result = $pdo->exec($sql);
	if(false === $result){
		echo "[SQL exec failed!] [$sql]\n";
	}else{
		echo "$result rows deleted!\n";
	}

	pdo_close($pdo);
	pid_unlock();
}

//获取命令参数
function command_init(){
	$last_id = $_SERVER['argc'] - 1;
	if($last_id <= 0){
		return ;
	}
	$command = $_SERVER['argv'][$last_id];
	return $command;
}

//获取命令选项信息, 长选项要以数组存储
function options_init($option_list){	
	$option_short = '';
	$option_long = array();	
	
	foreach ($option_list as $item){
		$option_flag = '';
		if($item[2] == 1) $option_flag = ':';
		if (isset($item[0])) $option_long[] = $item[0].$option_flag;
		if (isset($item[1])) $option_short .= $item[1].$option_flag;
	}
	
	$ret = array();
	$arr = getopt($option_short, $option_long);
	
	foreach ($arr as $key => $val){
		foreach ($option_list as $item){
			if ($key == $item[0] 
				|| ($key == $item[1] && !empty($item[0]))) {
				$ret[$item[0]] = empty($val) ? true : $val;
				break;
			}else if ($key == $item[1] && empty($item[0])){
				$ret[$item[1]] = empty($val) ? true : $val;
				break;
			}			
		}
	}
	
	return $ret;
}

//获取配置文件信息
function configs_init($config_file){
	$ret = array();

	if(!file_exists($config_file))
		return false;

	if($fp = fopen($config_file, 'r')){
		while($buffer = fgets($fp)){
			$buffer = trim($buffer);
			if(empty($buffer))
				continue;
			if(substr($buffer, 0, 1) == '#')
				continue;

			if($list = explode('=', $buffer)){
				$key = trim($list[0]);
				$value = trim($list[1]);

				if(!empty($key) && !empty($value)){
					$ret[$key] = $value;
				}
			}
		}
	}

	return $ret;
}

//使用帮助
function display_usage(){
	printf("usage: %s [options] command\n", S_PROJECT_NAME);
	printf("command:\n");
	printf("\n");
	printf("build: Build index Database\n");
	printf("clean: Clean index Database\n");		
	printf("update: Update index Database\n");
	printf("\n");
	printf("options:\n");
	printf("  -c [config file], --config=[config file]\n");
	printf("                        config file location\n");
	printf("  -v, --version         display version information\n");
	printf("  -h, --help            display usage information\n");
}

//版本信息
function display_version(){
	printf("%s v%d.%d\n", S_PROJECT_NAME, S_VERSION_MAJOR, S_VERSION_MINOR);
	printf("copyright (c) HZP, all rights reserved!\n");
}

//锁定进程
function pid_lock(){
	if (file_exists(S_PID_FILE)){
		$pid = file_get_contents(S_PID_FILE);
		$r = exec('ps -ax | awk \'{print $1}\' | grep -e "^'.$pid.'$"');
		
		if ($r == $pid){
			echo "already running, pid: $pid\n";
			exit();
		}
		
		unlink(S_PID_FILE);
	}
	
	$pid = getmypid();
	file_put_contents(S_PID_FILE, $pid);
}

function pid_unlock(){
	unlink(S_PID_FILE);	
}
////////
//post方式按天采集数据
function data_collect($date, $url, $pdo, $rule, $conditions, $method='post'){
	//判断是否为一年前日期
	if(!is_before_one_year($date)){
		//最近一年数据
		$conditions['searchRangeRadio'] = 0;
		$conditions['searchRange'] = 0;
	}else{
		//历史数据
		$conditions['searchRangeRadio'] = -1;
		$conditions['searchRange'] = -1;
	}
	
	$post = "condition.loginType=&condition.isPub=1&condition.type=&condition.dateFrom=$date&condition.dateTo=$date&pageNo=1&condition.provinceCode=
		&condition.plate=&condition.searchType={$conditions['searchType']}&condition.questioner=&condition.status={$conditions['status']}&condition.searchRange={$conditions['searchRange']}&condition.searchRangeRadio={$conditions['searchRangeRadio']}";
	$contents =  getHttpContent($url, $method, $post);
	if(!$contents){
		echo "[data_collect]ERROR: content false\n";
		return false;
	}
	if($pages = rule_match($rule['pages'], $contents)){

		echo '[' . $date . '] ' . '共 ' . $pages . ' 页' . "\n";
		for($pageNo=$pages; $pageNo>=1; --$pageNo){

			$post = "condition.loginType=&condition.isPub=1&condition.type=&condition.dateFrom=$date&condition.dateTo=$date&pageNo=$pageNo&condition.provinceCode=
			&condition.plate=&condition.searchType={$conditions['searchType']}&condition.questioner=&condition.status={$conditions['status']}&condition.searchRange={$conditions['searchRange']}&condition.searchRangeRadio={$conditions['searchRangeRadio']}";
			//去掉post中空白符
			$post = str_replace("\r\n\t", '', $post);

			$contents =  getHttpContent($url, $method, $post);
			if(!$contents){
				echo "[data_collect]ERROR: content false\n";
				return false;
			}

			//检测字符编码
			$encode = mb_detect_encoding($contents);
			//echo $encode;
			$contents = iconv($encode, "utf-8//IGNORE", $contents);
			$itemlist = page2itemlist($contents, $rule);
			$itemlist = column_sort('question_time', $itemlist);
			$ret = data_in($itemlist, $pdo);
			if(!$ret){
				echo "[data_collect]ERROR: data_in false\n";
				return false;
			}

		}
	}
	return true;
	
}

/**将网页数据转为数据库对应字段数据
 *@return 二维数组
 */
function page2itemlist($contents, $rule){
	$result = rule_matches($rule['list'], $contents);
	$itemlist = array();
	if(!empty($result)){
		foreach ($result as $value) {
			$item['question_id'] = trim(rule_match($rule['question_id'], $value));
			$item['mktseccode'] = trim(rule_match($rule['mktseccode'], $value));
			$item['stockname'] = trim(rule_match($rule['stockname'], $value));
			$item['question_time'] = trim(rule_match($rule['question_time'], $value));
			$item['answer_time'] = trim(rule_match($rule['answer_time'], $value));
			$item['question'] = trim(rule_match($rule['question'], $value));
			$item['questioner'] = trim(rule_match($rule['questioner'], $value));
			$item['answer'] = trim(rule_match($rule['answer'], $value));
			$item['respondent'] = trim(rule_match($rule['respondent'], $value));
			//$item['ctime'] = date('Y-m-d', strtotime($item['ctime']));
			//$item['mtime'] = $item['mtime'] ? date('Y-m-d', strtotime($item['mtime'])) : 0;
			
			$answerflag = trim(rule_match($rule['answerflag'], $value));
			if($answerflag == '已回复'){
				$item['answerflag'] = 1;
			}else{
				$item['answerflag'] = 0;
			}

			$code_prefix = substr($item['mktseccode'], 0, 2);
			if($code_prefix == '60' || $code_prefix == '50' || $code_prefix == '90'){
				$item['mktseccode'] = 'SH' . $item['mktseccode'];
			}elseif ($code_prefix == '00' || $code_prefix == '30' || $code_prefix == '20') {
				$item['mktseccode'] = 'SZ' . $item['mktseccode'];
			}

			$item['question'] = strip_tags($item['question']);
			$item['answer']   = strip_tags($item['answer']);

			$itemlist[] = $item;
		}
	}
	return $itemlist;
}

/**将采集数据存入数据库
 *@param itemlist 二维数组（外层多条记录，每条记录中多个字段对应数据库表中字段）
 */
function data_in($itemlist, $pdo){
	if(!empty($itemlist)){
		//$qtime = 0;
		//$atime = 0;
		foreach ($itemlist as $item) {
			if(strlen($item['mktseccode']) <= 3){
				echo 'Stock Code Not Exits!' . "\n";
				continue;
			}
			//入库前转义字符串
			$item = quote($item, $pdo);
			
			$sql_search_qid = "SELECT id,answerflag FROM ircs_main WHERE question_id={$item['question_id']}";

			if(!$result = $pdo->query($sql_search_qid)){
				echo "SQL Search ERROR: [{$item['mktseccode']}]---{$item['question_time']}\n";
				break;
			}

			$ret = $result->fetch(PDO::FETCH_ASSOC);
			if(!empty($ret)){
				//找到之前未回复的记录, 现在有回复, 则更新记录
				if($ret['answerflag'] == 0 && $item['answerflag'] == 1){
					$sql_update = "UPDATE ircs_main SET mtime=now(), answer_time={$item['answer_time']}, answer={$item['answer']}, answerflag=1, respondent={$item['respondent']} WHERE question_id={$item['question_id']}";
					if(false === $pdo->exec($sql_update)){
						echo 'SQL UPDATE ERROR: ' . '[' . $item['mktseccode'] . ']' . $item['question_id'] . '--' . $item['question_time'] . "\n";
						return false;
					}

				}
			}else{
				$sql = "INSERT INTO ircs_main(ctime, mtime, question_id, mktseccode, stockname, question_time, answer_time, question, questioner, answer, respondent, answerflag) VALUES(
				now(), now(), {$item['question_id']}, {$item['mktseccode']}, {$item['stockname']}, {$item['question_time']}, {$item['answer_time']}, {$item['question']}, 
				{$item['questioner']}, {$item['answer']}, {$item['respondent']}, {$item['answerflag']}
				)";

				if(false === $pdo->exec($sql)){
					echo 'SQL INSERT ERROR: ' . '[' . $item['mktseccode'] . ']' . $item['question_id'] . '--' . $item['question_time'] . "\n";
					return false;
				}
			}			
		}
	}
	return true;
}

//采集atime之后的已回复数据,按时间顺序采集
function answered_collect($date, $url, $atime, $rule, $pdo){
	echo "*[answered_collect] Begin**\n";
	$date_from = date("Y-m-d", strtotime('-1 month'));
	$page=1;
	//实时post
	$post_base = "condition.dateFrom={$date_from}&condition.dateTo={$date}&condition.status=&condition.keyWord=&condition.stockcode=&condition.searchType=
	&condition.questionCla=&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=&condition.provinceCode=&condition.plate=&pageNo=";
	/*
	//历史 已回复post
	condition.dateFrom=2015-08-14&condition.dateTo=2015-08-14&condition.status=3&condition.keyWord=&condition.stockcode=&condition.searchType=name&condition.questionCla=
	&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=-1&condition.provinceCode=&condition.plate=&pageNo=1
	*/

	//最近一年已回复post,
	/*
	$apost_base = "condition.dateFrom={$date}&condition.dateTo={$date}&condition.status=3&condition.keyWord=&condition.stockcode=&condition.searchType=name&condition.questionCla=
	&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=&condition.provinceCode=&condition.plate=&pageNo=";
	*/
	

	$post = $post_base . $page;

	$pageflag = 0;
	$content = getHttpContent($url);
	if(!$content){
		echo "Line:" . __LINE__ . "[answered_collect]ERROR: Content false\n";
		return ;
	}

	//查找上次记录时间所在的页
	if($pages = rule_match($rule['pages'], $content)){
		for($page=1; $page<=$pages; ++$page){
			$post = $post_base . $page;
			$content = getHttpContent($url, 'post', $post);
			if(!$content){
				echo "Line:" . __LINE__ . "[answered_collect]ERROR: Content false\n";
				continue;
			}
			$itemlist = page2itemlist($content, $rule);

			//查找最后答复时间所在的页数
			foreach ($itemlist as $row) {
				if(strtotime($row['answer_time']) <= strtotime($atime)){
					$pageflag = $page;
					break 2;
				}					
			}
		}

		if($pageflag == 0){
			//未找到记录时间对应页
			//$qtime = kv_get(S_OPT_FILE, 'question_time');
			$date_from = date("Y-m-d", strtotime("$atime -2 month"));
			$date_to = date("Y-m-d");

			while(strtotime($date_from) <= strtotime($date_to)){
				$page=1;
				$date = $date_from;
				$apost_base = "condition.dateFrom={$date}&condition.dateTo={$date}&condition.status=3&condition.keyWord=&condition.stockcode=&condition.searchType=name&condition.questionCla=
				&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=&condition.provinceCode=&condition.plate=&pageNo=";
				$post = $apost_base . $page;
				//echo "[answered_collect]POST: $post\n";
				echo "[$date]\n";
				$content = getHttpContent($url, 'post', $post);
				if(!$content){
					echo "Line:" . __LINE__ . "[answered_collect]ERROR: Content false\n";
					return ;
				}
				if($pages = rule_match($rule['pages'], $content)){
					for($page=$pages; $page>0; --$page){
						//获取记录中答复时间
						$atime = kv_get(S_OPT_FILE, 'answer_time');
						$post = $apost_base . $page;
						$content = getHttpContent($url, 'post', $post);
						if(!$content){
							echo "Line:" . __LINE__ . "[answered_collect]ERROR: Content false\n";
							return ;
						}
						$itemlist = page2itemlist($content, $rule);
						$itemlist = column_sort('answer_time', $itemlist);
						$atime_last = _insert_answered($itemlist, $pdo);

						if($atime_last != 0 && strtotime($atime_last) > strtotime($atime)){
							kv_set(S_OPT_FILE, 'answer_time', $atime_last);
						}
					}
				}
				$date_from = date_increment($date_from);
			}
			//...
			
		}else{
			for($page=$pageflag; $page>=1; --$page){

				$post = $post_base . $page;
				$content = getHttpContent($url, 'post', $post);
				if(!$content){
					echo "Line:" . __LINE__ . "[answered_collect]ERROR: Content false\n";
					break;
				}

				//echo "*DEBUG: [$page]\n";
				$itemlist = page2itemlist($content, $rule);
				$itemlist = column_sort('answer_time', $itemlist);
				$atime_last = _insert_answered($itemlist, $pdo);
				//echo "*DEBUG: [$page] end _insert_answered\n";

				if($atime_last){
					kv_set(S_OPT_FILE, 'answer_time', $atime_last);
				}
			}
		}
	}
}

//采集qtime之后的未回复数据,按时间顺序采集
function unanswered_collect($date, $url, $qtime, $rule, $pdo){
	echo "*[unanswered_collect] Begin**\n";
	$page=1;
	$pageflag = 0;

	$post_base = "condition.dateFrom={$date}&condition.dateTo={$date}&condition.status=1&condition.keyWord=&condition.stockcode=&condition.searchType=name&condition.questionCla=
	&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=0&condition.provinceCode=&condition.plate=&pageNo=";

	/*
	//未回复最近一年内post

	*/


	$post_unanswered = $post_base . $page;

	$content = getHttpContent($url, 'post', $post_unanswered);
	if(!$content){
		echo "Line:" . __LINE__ . "[unanswered_collect]ERROR: Content false\n";
		return ;
	}

	if($pages = rule_match($rule['pages'], $content)){

		for($page=1; $page<=$pages; ++$page){
			$post_unanswered = $post_base . $page;
			$content = getHttpContent($url, 'post', $post_unanswered);
			if(!$content){
				echo "Line:" . __LINE__ . "[unanswered_collect]ERROR: Content false\n";
				return ;
			}
			$itemlist = page2itemlist($content, $rule);

			//查找最后提问时间所在的页数
			foreach ($itemlist as $row) {
				if(strtotime($row['question_time']) <= strtotime($qtime)){
					$pageflag = $page;
					break 2;
				}					
			}
		}

		if($pageflag == 0){
			//从记录的时间开始一直到现在
			$date_from = date("Y-m-d", strtotime($qtime));
			$date_to = date("Y-m-d");
			
			while(strtotime($date_from) <= strtotime($date_to)){
				$page=1;
				$date = $date_from;
				$qpost_base = "condition.dateFrom={$date}&condition.dateTo={$date}&condition.status=1&condition.keyWord=&condition.stockcode=&condition.searchType=name&condition.questionCla=
				&condition.questionAtr=&condition.marketType=&condition.questioner=&condition.searchRange=0&condition.provinceCode=&condition.plate=&pageNo=";
				$post = $qpost_base . $page;
				//echo "[unanswered_collect]POST: $post\n";
				$content = getHttpContent($url, 'post', $post);
				if(!$content){
					echo "Line:" . __LINE__ . "[unanswered_collect]ERROR: Content false\n";
					return ;
				}
				if($pages = rule_match($rule['pages'], $content)){
					for($page=$pages; $page>0; --$page){
						$post = $qpost_base . $page;
						$content = getHttpContent($url, 'post', $post);
						if(!$content){
							echo "Line:" . __LINE__ . "[unanswered_collect]ERROR: Content false\n";
							//...
							return ;
						}
						$itemlist = page2itemlist($content, $rule);
						$itemlist = column_sort('question_time', $itemlist);
						$qtime_last = _insert_unanswered($itemlist, $pdo);

						if($qtime_last != 0 ){
							kv_set(S_OPT_FILE, 'question_time', $qtime_last);
						}
					}
				}
				$date_from = date_increment($date_from);
			}
		}else{
			//未回复页面数据不稳定，增加扫描前一页
			for($page=$pageflag+1; $page>=1; --$page){
				$post_unanswered = $post_base . $page;
				$content = getHttpContent($url, 'post', $post_unanswered);
				if(!$content){
					echo "Line:" . __LINE__ . "[unanswered_collect]ERROR: Content false\n";
					//...
					return ;
				}
				$itemlist = page2itemlist($content, $rule);
				$itemlist = column_sort('question_time', $itemlist);
				$qtime_last = _insert_unanswered($itemlist, $pdo);

				if($qtime_last != 0){
					kv_set(S_OPT_FILE, 'question_time', $qtime_last);
				}
			}

		}
		
	}
}

//插入已回复数据
function _insert_answered($itemlist, $pdo){
	//echo "**[_insert_answered]Insert Answered Data\n";
	$atime_last = 0;
	if(empty($itemlist))
		return $atime_last;
	foreach ($itemlist as $item) {
		//股票不存在
		if(strlen($item['mktseccode']) <= 3)
			continue;

		$item = quote($item, $pdo);
		$sql = "SELECT id,answerflag,question_id FROM ircs_main WHERE question_id={$item['question_id']}";
		if(!$result = $pdo->query($sql)){
			echo "SQL Search ERROR: [{$item['mktseccode']}]---{$item['question_time']}\n";
			break;
		}

		$ret = $result->fetch(PDO::FETCH_ASSOC);
		
		if(empty($ret)){
			//直接插入
			$sql_insert = "INSERT INTO ircs_main(ctime, mtime, question_id, mktseccode, stockname, question_time, answer_time, question, questioner, answer, respondent, answerflag) VALUES(
			now(), now(), {$item['question_id']}, {$item['mktseccode']}, {$item['stockname']}, {$item['question_time']}, {$item['answer_time']}, {$item['question']}, 
			{$item['questioner']}, {$item['answer']}, {$item['respondent']}, {$item['answerflag']}
			)";
			if(false === $pdo->exec($sql_insert)){
				echo 'SQL INSERT ERROR: ' . '[' . $item['mktseccode'] . ']' . $item['question_id'] . '--' . $item['question_time'] . "\n";
				echo "SQL: $sql_insert\n";
				break;
			}
			$atime_last = $item['answer_time'];

		}elseif ($ret['answerflag'] == 0) {
			//echo "Update: {$ret['question_time']}--{$ret['question_id']}--{$item['question_id']}\n";
			//更新
			$sql_update = "UPDATE ircs_main SET mtime=now(), answer_time={$item['answer_time']}, answer={$item['answer']}, answerflag=1, respondent={$item['respondent']} WHERE question_id={$item['question_id']}";
			if(false === $pdo->exec($sql_update)){
				echo 'SQL UPDATE ERROR: ' . '[' . $item['mktseccode'] . ']' . $item['question_id'] . '--' . $item['question_time'] . '--'. $item['answer_time'] ."\n";
				break;
			}
			$atime_last = $item['answer_time'];
		}

	}

	if($atime_last){
		//去掉引号
		$atime_last = substr($atime_last, 1, strlen($atime_last)-2);
	}

	return $atime_last;
}

//插入未回复数据
function _insert_unanswered($itemlist, $pdo){
	//echo "**[_insert_unanswered]Insert Unanswered Data\n";
	$qtime_last = 0;
	
	if(empty($itemlist))
		return $qtime_last;
	foreach ($itemlist as $item) {
		if(strlen($item['mktseccode']) <= 3)
			continue;
		$item = quote($item, $pdo);

		$sql = "SELECT id FROM ircs_main WHERE question_id={$item['question_id']}";
		if(!$result = $pdo->query($sql)){
			echo "SQL Search ERROR: [{$item['mktseccode']}]---{$item['question_time']}\n";
			break;
		}

		$ret = $result->fetch(PDO::FETCH_ASSOC);
		if(empty($ret)){
			$sql_insert = "INSERT INTO ircs_main(ctime, mtime, question_id, mktseccode, stockname, question_time, answer_time, question, questioner, answer, respondent, answerflag) VALUES(
			now(), now(), {$item['question_id']}, {$item['mktseccode']}, {$item['stockname']}, {$item['question_time']}, {$item['answer_time']}, {$item['question']}, 
			{$item['questioner']}, {$item['answer']}, {$item['respondent']}, {$item['answerflag']}
			)";
			if(false === $pdo->exec($sql_insert)){
				echo 'SQL INSERT ERROR: ' . '[' . $item['mktseccode'] . ']' . $item['question_id'] . '--' . $item['question_time'] . "\n";
				break;
			}
			$qtime_last = $item['question_time'];
			//echo "$qtime_last\n";
		}
	}
	if($qtime_last){
		//去掉引号
		$qtime_last = substr($qtime_last, 1, strlen($qtime_last)-2);
	}
	return $qtime_last;
}

//单个匹配
function rule_match($rule, $content){
	if(preg_match($rule, $content, $matche)){
		return $matche[1];
	}
	return 0;
}
//多个匹配
function rule_matches($rule, $content){
	$itemlist = array();
	if(preg_match_all($rule, $content, $matches)){
		foreach ($matches[1] as $value) {
			$itemlist[] = $value;
		}
	}
	return $itemlist;
}

//转义sql语句中的所有字段字符串
function quote($arr_str, $pdo){
	foreach ($arr_str as $key => $str) {
		$arr_str[$key] = $pdo->quote($str);
	}
	return $arr_str;
}

//获取日期时间段
function input_date($configs){
	//检查配置文件中是否设置日期
	if(isset($configs['date_from']) && isset($configs['date_to'])){
		$date['date_f'] = $configs['date_from'];
		$date['date_t'] = $configs['date_to']; 
	}else{
		echo "输入起始日期(格式[2016-01-01],输入0或空时，表示从2008-01-01开始至今)：\n";
		fscanf(STDIN, "%s", $date_from);
		if(!$date_from){
			$date_from = '2008-01-01';
			$date_to = date("Y-m-d", time());
		}else{
			echo "输入截止日期：\n";
			fscanf(STDIN, "%s", $date_to);
		}
		$date['date_f'] = $date_from;
		$date['date_t'] = $date_to;
	}

	if(!check_date($date['date_f']) || !check_date($date['date_t'])){
		echo "日期格式不正确！\n";
		return -1;
	}
	
	return $date;
}

//判断日期格式是否合法
function check_date($date){
	//能否转换时间戳
	$timestamp = strtotime($date);
	if(!$timestamp){
		return false;
	}
	//格式是否Y-m-d
	$new_date = date("Y-m-d", $timestamp);
	if($date == $new_date){
		return true;
	}
	return false;
}

/**判断一个日期是否为一年前，是返回1，否返回0
 *date 格式2016-08-03
 *
 */
function is_before_one_year($date){
	$one_year = date("Y-m-d", strtotime("-1 year"));
	if(strtotime($date) <= strtotime($one_year)){
		return 1;
	}
	return 0;
}

function date_increment($date){
	//格式2016-07-30
	return date("Y-m-d", strtotime($date) + 24*60*60);
}


//对表结构数组按字段值排序，升序排序
function column_sort($col, $arr){
	foreach ($arr as $key => $value) {
		$col_list[$key] = $value[$col];
	}
	if(!array_multisort($col_list, $arr)){
		return false;
	}
	return $arr;
}

//key=value格式文件读取
function kv_read($file){
	$ret = array();
	if(!file_exists($file)){
		return false;
	}

	if($fp = fopen($file, 'r')){
		while($buffer = fgets($fp)){
			if(empty($buffer))
				continue;

			$list = explode('=', $buffer);
			if(count($list) == 1)
				continue;

			$key = trim($list[0]);
			$value = trim($list[1]);

			if(empty($key) || empty($value))
				continue;

			$ret[$key] = $value;
		}
		fclose($fp);
	}

	return $ret;
}


//key=value格式写入文件
function kv_write($file, $arr){
	if($fp = fopen($file, 'w')){
		foreach ($arr as $key => $value) {
			$buffer = "$key=$value\n";
			fputs($fp, $buffer);
		}
		fclose($fp);
	}
}

function kv_get($file, $key){
	$arr = kv_read($file);
	if (array_key_exists($key, $arr))
		return $arr[$key];
}
	
function kv_set($file, $key, $val){
		$arr = kv_read($file);
		$arr[$key] = $val;
		kv_write($file, $arr);
}

//递归创建目录
function my_mkdir($dir){
	if(!is_dir($dir)){
		if(!my_mkdir(dirname($dir))){
			return false;
		}

		if(!mkdir($dir)){
			return false;
		}
	}
	return true;
}


/* pdo连接数据库 */
function pdo_connect($configs){
	try {
		$pdo = new PDO("$configs[db_driver]:host=$configs[db_host];dbname=$configs[db_name]",$configs['db_account'],$configs['db_password']);
		$pdo->query('SET NAMES '.$configs['db_charset']);
	} catch (Exception $e) {
		echo "[PDOconnect]ERROR: Database connect failed:\n";
		echo $e->getMessage() . "\n";
		exit();
	}
	
	return $pdo;
}

function pdo_close(&$pdo){
	$pdo = null;	
}

/* 通过 Http 取数据 */
function getHttpContent($url,$method="get",$post="",$returnHeader=false,$timeout=15){
	$header = array();
	$body = '';
		
	$matches = parse_url($url);
	$host = $matches['host'];
	$path = $matches['path'] ? $matches['path'].(isset($matches['query']) ? '?'.$matches['query'] : '') : '/';
	$port = !empty($matches['port']) ? $matches['port'] : 80;

	if(strtolower($method)=='post') {
		$out = "POST $path HTTP/1.1\r\n";
		$out .= "Accept: */*\r\n";
		$out .= "Accept-Language: zh-cn\r\n";
		$out .= "Content-Type: application/x-www-form-urlencoded\r\n";
		if (isset($_SERVER['HTTP_USER_AGENT'])) $out .= "User-Agent: $_SERVER[HTTP_USER_AGENT]\r\n";
		$out .= "Host: $host\r\n";
		$out .= 'Content-Length: '.strlen($post)."\r\n";
		$out .= "Connection: Close\r\n";
		$out .= "Cache-Control: no-cache\r\n";
		if (isset($GLOBALS['HTTP_COOKIE'])) $out .= "Cookie: $GLOBALS[HTTP_COOKIE]\r\n";		
		$out .= "\r\n";
		$out .= $post;
	} else {
		$out = "GET $path HTTP/1.1\r\n";
		$out .= "Accept: */*\r\n";
		$out .= "Accept-Language: zh-cn\r\n";
		if (isset($_SERVER['HTTP_USER_AGENT'])) $out .= "User-Agent: $_SERVER[HTTP_USER_AGENT]\r\n";
		$out .= "Host: $host\r\n";
		$out .= "Connection: Close\r\n";
		if (isset($GLOBALS['HTTP_COOKIE'])) $out .= "Cookie: $GLOBALS[HTTP_COOKIE]\r\n";		
		$out .= "\r\n";
	}
	
	if (isset($GLOBALS['HTTP_SOCKET5_CONFIG'])) $socket5_config=$GLOBALS['HTTP_SOCKET5_CONFIG'];
	if (isset($socket5_config) && is_array($socket5_config) && isset($socket5_config['host'])){
		$fp = @fsocket5open($socket5_config,$host, $port, $errno, $errstr, $timeout);
	}else{
		$fp = @fsockopen(gethostbyname($host), $port, $errno, $errstr, $timeout);
	}

	if(!$fp) return false;
	stream_set_blocking($fp, true);
	stream_set_timeout($fp, $timeout);

	@fwrite($fp, $out);
	$status = stream_get_meta_data($fp);

	if(!$status['timed_out']) {
		while (!feof($fp)) {
			$buffer = @fgets($fp);
			if (preg_match('/^HTTP\/(\d\.\d) (\d+) (\w+)/is',$buffer,$matches)){
				$header['version']=$matches[1];
				$header['status']=intval($matches[2]);
				$header['info']=$matches[3];
			}else if ($pos=strpos($buffer,':')){
				$header[trim(substr($buffer,0,$pos))]=trim(substr($buffer,$pos+1));
			}
			
			if($buffer=="\r\n" ||  $buffer=="\n") break;
		}
		if (!$returnHeader){
			if (isset($header['Transfer-Encoding']) && $header['Transfer-Encoding']=='chunked'){
				$chunk_size = (integer)hexdec(fgets($fp,1024));
				while(!feof($fp) && $chunk_size > 0) {

					$buffer=fread($fp, $chunk_size);
					if(!$buffer){
						echo "ERROR: **[getHttpContent] read failed\n";
						return false;
					}
					$body.=$buffer;
					$chunk_size -= strlen($buffer);
					
					if ($chunk_size>0){
						//echo "BUFFER: $buffer\n";
						continue;
					}
					
					fread($fp,2); // skip \r\n
					
					$chunk_size=(integer)hexdec(fgets($fp, 1024));
					
				}
				
			}else{								
				while(!feof($fp)) {
					$buffer = fread($fp, 8192);
					$body .= $buffer;
				}
			}
		}

	}

	@fclose($fp);
	return $returnHeader?$header:$body;
}