<?php

/*
require "/home/test/xunsearch/sdk/php/lib/XS.php";

$itemlist[] = array('atime'=>'2016-08-19 09:46:42', 'id'=>4 , 'message'=>"hello\nworld");
$itemlist[] = array('atime'=>'2016-08-19 09:45:34', 'id'=>2 , 'message'=>"hello\rworld");
$itemlist[] = array('atime'=>'2016-08-19 09:45:04', 'id'=>1 , 'message'=>"hello world");
$itemlist[] = array('atime'=>'2016-08-19 09:45:33', 'id'=>3 , 'message'=>"hello, \n\rworld");

$xs = new XS('test');
$xsi = $xs->index;
foreach ($itemlist as $item) {
	$doc = new XSDocument;
	$doc->setFields($item);
	$xsi->add($doc);
}

*/

$date = "2015-03-01";
$time = strtotime("$date -2 month");

echo date("Y-m-d\n", $time);
echo "line: " . __LINE__ . "\n";

// $url = "http://ircs.p5w.net/ircs/interaction/queryQuestionByGszz.do";
// get_http_data($url);



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


function get_http_data($url, $method='post', $post='', $timeout=15){

	$header = array();
	$body = '';
		
	$matches = parse_url($url);
	$host = $matches['host'];
	$path = $matches['path'] ? $matches['path'].(isset($matches['query']) ? '?'.$matches['query'] : '') : '/';
	$port = !empty($matches['port']) ? $matches['port'] : 80;

	if(strtolower($method) == 'post'){
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

	$fp = @fsockopen(gethostbyname($host), $port, $errno, $errstr, $timeout);

	if(!$fp){
		return false;
	}

	stream_set_blocking($fp, true);
	stream_set_timeout($fp, $timeout);

	fwrite($fp, $out);
	$status = stream_get_meta_data($fp);

	if(!$status['timed_out']){
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

		if ($header['Transfer-Encoding']=='chunked'){

			$chunk_size=(integer)hexdec(fgets($fp, 1024));
			while(!feof($fp) && $chunk_size > 0){
				$buffer = fread($fp, $chunk_size);
				echo $buffer;
				$chunk_size -= strlen($buffer);
				if($chunk_size > 0){
					continue;
				}
			}
			
		}else{
			echo "**[getHttpContent] Begin else while\n";
		}
	}
	fclose($fp);

}