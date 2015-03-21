package com.melody.spider.worker;

import java.util.logging.Logger;

import com.melody.spider.fetcher.PageFetcher;
import com.melody.spider.handler.ContentHandler;
import com.melody.spider.model.FetchedPage;
import com.melody.spider.model.SpiderParams;
import com.melody.spider.parser.ContentParser;
import com.melody.spider.queue.UrlQueue;
import com.melody.spider.storage.DataStorage;

public class SpiderWorker implements Runnable{
		private static final Logger Log = Logger.getLogger(SpiderWorker.class.getName());
		private PageFetcher fetcher;
		private ContentHandler handler;
		private ContentParser parser;
		private DataStorage store;
		private int threadIndex;
		
		public SpiderWorker(int threadIndex){
			this.threadIndex = threadIndex;
			this.fetcher = new PageFetcher();
			this.handler = new ContentHandler();
			this.parser = new ContentParser();
			this.store = new DataStorage();
		}
		
		@Override
		public void run() {
			// 登录
			
			
			// 当待抓取URL队列不为空时，执行爬取任务
			// 注： 当队列内容为空时，也不爬取任务已经结束了
			//     因为有可能是UrlQueue暂时空，其他worker线程还没有将新的URL放入队列
			//	        所以，这里可以做个等待时间，再进行抓取（二次机会）
			while(!UrlQueue.isEmpty()){
				// 从待抓取队列中拿URL
				String url = UrlQueue.outElement();
				
				// 抓取URL指定的页面，并返回状态码和页面内容构成的FetchedPage对象
				FetchedPage fetchedPage = fetcher.getContentFromUrl(url);
				
				// 检查爬取页面的合法性，爬虫是否被禁止
				if(!handler.check(fetchedPage)){
					// 切换IP等操作
					// TODO
					
					Log.info("Spider-" + threadIndex + ": switch IP to ");
					continue;
				}
				
				// 解析页面，获取目标数据
				Object targetData = parser.parse(fetchedPage);
				
				// 存储目标数据到数据存储（如DB）、存储已爬取的Url到VisitedUrlQueue
				store.store(targetData);
				
				// delay
				try {
					Thread.sleep(SpiderParams.DEYLAY_TIME);
				} 
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			fetcher.close();
			Log.info("Spider-" + threadIndex + ": stop...");
		}

}
