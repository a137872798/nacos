/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.selector;


import com.alibaba.nacos.api.cmdb.pojo.PreservedEntityTypes;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.selector.ExpressionSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.cmdb.service.CmdbReader;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.Instance;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A selector to implement a so called same-label-prior rule for service discovery.
 * <h2>Backgroup</h2>
 * Consider service providers are deployed in two sites i.e. site A and site B, and consumers
 * of this service provider are also deployed in site A and site B. So the consumers may want to
 * visit the service provider in current site, thus consumers in site A visit service providers
 * in site A and consumers in site B visit service providers in site B. This is quite useful to
 * reduce the transfer delay of RPC. This is called same-site-prior strategy.
 * <h2>Same Label Prior</h2>
 * The same-site-prior strategy covers many circumstances in large companies and we can abstract
 * it to a higher level strategy: same-label-prior.
 * <p>
 * So the idea is that presumed we have built a self-defined or integrated a third-party idc CMDB
 * which stores all the labels of all IPs. Then we can filter provider IPs by the consumer IP and
 * we only return the providers who have the same label values with consumer. We can define the
 * labels we want to include in the comparison.
 * <p>
 * If no provider has the same label value with the consumer, we fall back to give all providers
 * to the consumer. Note that this fallback strategy may also be abstracted in future to introduce
 * more kinds of behaviors.
 *
 * @author nkorange
 * @see CmdbReader
 * @since 0.7.0
 * 基于标签选择
 */
public class LabelSelector extends ExpressionSelector implements Selector {

    /**
     * The labels relevant to this the selector.
     *
     * @see com.alibaba.nacos.api.cmdb.pojo.Label
     * 内部预存的一组标签对象
     */
    private Set<String> labels;

    /**
     * '='
     */
    private static final Set<String> SUPPORTED_INNER_CONNCETORS = new HashSet<>();

    /**
     * '&'
     */
    private static final Set<String> SUPPORTED_OUTER_CONNCETORS = new HashSet<>();

    private static final String CONSUMER_PREFIX = "CONSUMER.label.";

    private static final String PROVIDER_PREFIX = "PROVIDER.label.";

    private static final char CEQUAL = '=';

    private static final char CAND = '&';

    static {
        SUPPORTED_INNER_CONNCETORS.add(String.valueOf(CEQUAL));
        SUPPORTED_OUTER_CONNCETORS.add(String.valueOf(CAND));
    }

    public Set<String> getLabels() {
        return labels;
    }

    public void setLabels(Set<String> labels) {
        this.labels = labels;
    }

    public LabelSelector() {
        setType(SelectorType.label.name());
    }

    /**
     * 创建一个 reader 对象   该对象用于从 cmdb读取数据
     * @return
     */
    private CmdbReader getCmdbReader() {
        return SpringContext.getAppContext().getBean(CmdbReader.class);
    }

    /**
     * 解析表达式并生成一组 字符串
     * @param expression
     * @return
     * @throws NacosException
     */
    public static Set<String> parseExpression(String expression) throws NacosException {
        return ExpressionInterpreter.parseExpression(expression);
    }


    /**
     * 根据消费者地址 找到一组匹配的提供者
     * @param consumer  consumer address
     * @param providers candidate provider addresses
     * @return
     */
    @Override
    public List<Instance> select(String consumer, List<Instance> providers) {

        // 没有标签这种分类信息时 直接全数返回
        if (labels.isEmpty()) {
            return providers;
        }

        List<Instance> instanceList = new ArrayList<>();
        for (Instance instance : providers) {

            boolean matched = true;
            for (String labelName : getLabels()) {

                //根据消费者地址信息 以及 标签名 还有存储类型 找到标签值
                String consumerLabelValue = getCmdbReader().queryLabel(consumer, PreservedEntityTypes.ip.name(), labelName);

                //同样的方式找到提供者信息 并进行匹配
                if (StringUtils.isNotBlank(consumerLabelValue) &&
                        !StringUtils.equals(consumerLabelValue,
                            getCmdbReader().queryLabel(instance.getIp(), PreservedEntityTypes.ip.name(), labelName))) {
                    matched = false;
                    break;
                }
            }
            if (matched) {
                instanceList.add(instance);
            }
        }

        // 匹配失败时降级措施是返回所有的提供者
        if (instanceList.isEmpty()) {
            return providers;
        }

        return instanceList;
    }

    /**
     * Expression interpreter for label selector.
     * <p>
     * For now it supports very limited set of syntax rules.
     */
    public static class ExpressionInterpreter {

        /**
         * Parse the label expression.
         * <p>
         * Currently we support the very single type of expression:
         * <pre>
         *     consumer.labelA = provider.labelA & consumer.labelB = provider.labelB
         * </pre>
         * Later we will implement a interpreter to parse this expression in a standard LL parser way.
         *
         * @param expression the label expression to parse
         * @return collection of labels
         * 将表达式解析成一组标签
         */
        public static Set<String> parseExpression(String expression) throws NacosException {

            if (StringUtils.isBlank(expression)) {
                return new HashSet<>();
            }

            // 去除空格
            expression = StringUtils.deleteWhitespace(expression);

            // 将表达式打散成 token 以及子句
            List<String> elements = getTerms(expression);
            Set<String> gotLabels = new HashSet<>();
            int index = 0;

            // 按照键值对进行拆分
            index = checkInnerSyntax(elements, index);

            if (index == -1) {
                throw new NacosException(NacosException.INVALID_PARAM, "parse expression failed!");
            }

            // 找到provider.label 添加到容器中
            gotLabels.add(elements.get(index++).split(PROVIDER_PREFIX)[1]);

            // 简易的理解为不断地匹配下一个键值对 并将结果保存到list中
            while (index < elements.size()) {

                index = checkOuterSyntax(elements, index);

                if (index >= elements.size()) {
                    return gotLabels;
                }

                if (index == -1) {
                    throw new NacosException(NacosException.INVALID_PARAM, "parse expression failed!");
                }

                gotLabels.add(elements.get(index++).split(PROVIDER_PREFIX)[1]);
            }

            return gotLabels;
        }

        /***
         * 将表达式打散
         * @param expression
         * @return
         */
        public static List<String> getTerms(String expression) {

            List<String> terms = new ArrayList<>();

            Set<Character> characters = new HashSet<>();
            characters.add(CEQUAL);
            characters.add(CAND);

            char[] chars = expression.toCharArray();

            int lastIndex = 0;
            for (int index = 0; index < chars.length; index++) {
                char ch = chars[index];
                // 尝试匹配 CEQUAL CAND
                if (characters.contains(ch)) {
                    // 将子表达式 和 符号分别存放到 list 中
                    terms.add(expression.substring(lastIndex, index));
                    terms.add(expression.substring(index, index+1));
                    index ++;
                    lastIndex = index;
                }
            }

            // 将最后部分添加到 list 中
            terms.add(expression.substring(lastIndex, chars.length));

            return terms;
        }

        private static int skipEmpty(List<String> elements, int start) {
            while (start < elements.size() && StringUtils.isBlank(elements.get(start))) {
                start++;
            }
            return start;
        }

        private static int checkOuterSyntax(List<String> elements, int start) {

            int index = start;

            index = skipEmpty(elements, index);
            if (index >= elements.size()) {
                return index;
            }

            if (!SUPPORTED_OUTER_CONNCETORS.contains(elements.get(index++))) {
                return -1;
            }

            return checkInnerSyntax(elements, index);
        }

        /**
         * 检查内部语法
         * @param elements
         * @param start
         * @return
         */
        private static int checkInnerSyntax(List<String> elements, int start) {

            int index = start;

            index = skipEmpty(elements, index);
            if (index >= elements.size()) {
                return -1;
            }

            // 第一个标签必须是  consumer.label
            if (!elements.get(index).startsWith(CONSUMER_PREFIX)) {
                return -1;
            }

            // 获取剩下的部分
            String labelConsumer = elements.get(index++).split(CONSUMER_PREFIX)[1];

            index = skipEmpty(elements, index);
            if (index >= elements.size()) {
                return -1;
            }

            // 下一个元素必须是 '='
            if (!SUPPORTED_INNER_CONNCETORS.contains(elements.get(index++))) {
                return -1;
            }

            index = skipEmpty(elements, index);
            if (index >= elements.size()) {
                return -1;
            }

            // 找到 provider.label
            if (!elements.get(index).startsWith(PROVIDER_PREFIX)) {
                return -1;
            }

            String labelProvider = elements.get(index).split(PROVIDER_PREFIX)[1];

            //确保2个标签对应
            if (!labelConsumer.equals(labelProvider)) {
                return -1;
            }

            return index;
        }
    }
}
