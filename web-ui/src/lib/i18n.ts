export type Locale = 'zh' | 'en';

export interface Translations {
  common: {
    loading: string;
    error: string;
    success: string;
    warning: string;
    confirm: string;
    cancel: string;
    save: string;
    delete: string;
    edit: string;
    search: string;
    refresh: string;
    back: string;
    viewAll: string;
    noData: string;
    run: string;
    pause: string;
    resume: string;
    paused: string;
    home: string;
    yes: string;
    no: string;
  };
  nav: {
    dashboard: string;
    dags: string;
    history: string;
    settings: string;
  };
  dashboard: {
    title: string;
    subtitle: string;
    dagCount: string;
    activeRuns: string;
    totalTasks: string;
    systemStatus: string;
    running: string;
    stopped: string;
    quickActions: string;
    schedulerEngine: string;
    dags: string;
    failedTasks: string;
    successRate: string;
    moreMetrics: string;
    hideMetrics: string;
    searchPlaceholder: string;
    allDags: string;
    healthy: string;
    abnormal: string;
    runHistory: string;
    runs: string;
    noRecords: string;
    queueBacklog: string;
    runningInstances: string;
    recentSuccessRate: string;
    noMatchingDags: string;
  };
  dags: {
    title: string;
    subtitle: string;
    searchPlaceholder: string;
    totalCount: string;
    noDescription: string;
    taskCount: string;
    trigger: string;
    triggering: string;
    notFound: string;
    adjustSearch: string;
  };
  dagDetail: {
    backToDags: string;
    triggerRun: string;
    taskDefinitions: string;
    flowGraph: string;
    runInstances: string;
    xcomData: string;
    xcomDescription: string;
    selectRun: string;
    noRuns: string;
    clickTrigger: string;
    noXcom: string;
    showingRunStatus: string;
    selectRunToView: string;
    started: string;
    finished: string;
    dependsOn: string;
    executor: string;
    executionTime: string;
    variables: string;
  };
  runStatus: {
    running: string;
    success: string;
    failed: string;
    pending: string;
    upstreamFailed: string;
    retrying: string;
    skipped: string;
    noRun: string;
    inactive: string;
  };
  triggerType: {
    manual: string;
    schedule: string;
    api: string;
  };
  history: {
    title: string;
    subtitle: string;
    searchPlaceholder: string;
    allStatus: string;
    noRecords: string;
    executionDate: string;
    tasks: string;
    failed: string;
    inProgress: string;
    duration: string;
    records: string;
  };
  settings: {
    title: string;
    subtitle: string;
    systemStatus: string;
    systemStatusDesc: string;
    healthStatus: string;
    healthy: string;
    unhealthy: string;
    lastUpdated: string;
    resourceStats: string;
    resourceStatsDesc: string;
    totalDAGs: string;
    activeRuns: string;
    language: string;
    languageDesc: string;
    zh: string;
    en: string;
    notifications: string;
    clearAll: string;
    noNotifications: string;
    appearance: string;
    appearanceDesc: string;
    theme: string;
    themeDesc: string;
    themeLight: string;
    themeDark: string;
    themeSystem: string;
    selectLanguage: string;
    dataRefresh: string;
    dataRefreshDesc: string;
    refreshInterval: string;
    refreshIntervalDesc: string;
    interval3s: string;
    interval5s: string;
    interval10s: string;
    interval30s: string;
    intervalOff: string;
    notifyOnFailure: string;
    notifyOnFailureDesc: string;
    systemInfo: string;
    systemInfoDesc: string;
    webUIVersion: string;
    apiEndpoint: string;
    buildTime: string;
    themeUpdated: string;
    languageSwitchedZh: string;
    languageSwitchedEn: string;
    refreshDisabled: string;
    refreshIntervalSet: string;
    notifyEnabled: string;
    notifyDisabled: string;
  };
  notFound: {
    title: string;
    message: string;
    backHome: string;
    viewDAGs: string;
  };
  sidebar: {
    mainFeatures: string;
    system: string;
    apiConnected: string;
  };
  commandPalette: {
    placeholder: string;
    noResults: string;
    navigation: string;
    recentRuns: string;
  };
  toast: {
    dagTriggered: string;
    dagTriggerFailed: string;
    newRunCreated: string;
    fetchDagsFailed: string;
    fetchDagDetailFailed: string;
    fetchHistoryFailed: string;
    unknownError: string;
    cannotTriggerEmptyDAG: string;
    noDagTasks: string;
  };
}

const zhTranslations: Translations = {
  common: {
    loading: '加载中…',
    error: '错误',
    success: '成功',
    warning: '警告',
    confirm: '确认',
    cancel: '取消',
    save: '保存',
    delete: '删除',
    edit: '编辑',
    search: '搜索',
    refresh: '刷新',
    back: '返回',
    viewAll: '查看全部',
    noData: '暂无数据',
    run: '运行',
    pause: '暂停',
    resume: '恢复',
    paused: '已暂停',
    home: '首页',
    yes: '是',
    no: '否',
  },
  nav: {
    dashboard: '仪表盘',
    dags: 'DAGs',
    history: '运行历史',
    settings: '设置',
  },
  dashboard: {
    title: '仪表盘',
    subtitle: '任务调度总览',
    dagCount: 'DAG 数量',
    activeRuns: '运行中实例',
    totalTasks: '任务总数',
    systemStatus: '系统状态',
    running: '运行中',
    stopped: '已停止',
    quickActions: '快速操作',
    schedulerEngine: '调度引擎',
    dags: 'DAGs',
    failedTasks: '失败任务',
    successRate: '成功率',
    moreMetrics: '更多指标',
    hideMetrics: '收起详细指标',
    searchPlaceholder: '搜索 DAG 名称或描述...',
    allDags: '全部',
    healthy: '健康',
    abnormal: '异常',
    runHistory: '运行历史',
    runs: '次运行',
    noRecords: '无记录',
    queueBacklog: '队列积压',
    runningInstances: '个运行中',
    recentSuccessRate: '近期成功率',
    noMatchingDags: '未找到匹配的 DAG',
  },
  dags: {
    title: 'DAGs',
    subtitle: '查看所有工作流',
    searchPlaceholder: '搜索 DAG…',
    totalCount: '共 {count} 个 DAG',
    noDescription: '暂无描述',
    taskCount: '{count} 个任务',
    trigger: '运行',
    triggering: '触发中…',
    notFound: '未找到匹配的 DAG',
    adjustSearch: '尝试调整搜索条件',
  },
  dagDetail: {
    backToDags: '返回 DAG 列表',
    triggerRun: '触发运行',
    taskDefinitions: '任务定义',
    flowGraph: '流程图',
    runInstances: '运行实例',
    xcomData: 'XCom 数据',
    xcomDescription: '任务间传递的数据',
    selectRun: '选择运行实例',
    noRuns: '暂无运行实例',
    clickTrigger: '点击“触发运行”开始执行',
    noXcom: '该实例没有 XCom 数据',
    showingRunStatus: '显示 Run #{number} 的执行状态',
    selectRunToView: '选择运行实例查看状态',
    started: '开始',
    finished: '结束',
    dependsOn: '依赖任务',
    executor: '执行器',
    executionTime: '执行时长',
    variables: '个变量',
  },
  runStatus: {
    running: '运行中',
    success: '成功',
    failed: '失败',
    pending: '等待中',
    upstreamFailed: '上游失败',
    retrying: '重试中',
    skipped: '已跳过',
    noRun: '未运行',
    inactive: '未启用',
  },
  triggerType: {
    manual: '手动',
    schedule: '定时',
    api: 'API',
  },
  toast: {
    dagTriggered: '已触发 DAG',
    dagTriggerFailed: '触发 DAG 失败',
    newRunCreated: '已创建新的运行实例',
    fetchDagsFailed: '获取 DAG 列表失败',
    fetchDagDetailFailed: '获取 DAG 详情失败',
    fetchHistoryFailed: '获取运行历史失败',
    unknownError: '未知错误',
    cannotTriggerEmptyDAG: '无法触发 DAG',
    noDagTasks: '当前 DAG 没有任务',
  },
  history: {
    title: '运行历史',
    subtitle: '查看 DAG 执行记录',
    searchPlaceholder: '搜索 DAG…',
    allStatus: '所有状态',
    noRecords: '暂无记录',
    executionDate: '执行时间',
    tasks: '个任务',
    failed: '失败',
    inProgress: '进行中',
    duration: '耗时',
    records: '执行记录',
  },
  settings: {
    title: '系统设置',
    subtitle: '系统配置与状态',
    systemStatus: '系统状态',
    systemStatusDesc: '当前系统状态',
    healthStatus: '健康状态',
    healthy: '健康',
    unhealthy: '异常',
    lastUpdated: '最后更新',
    resourceStats: '资源统计',
    resourceStatsDesc: 'DAG 运行统计',
    totalDAGs: 'DAG 总数',
    activeRuns: '运行中实例',
    language: '语言设置',
    languageDesc: '选择界面语言',
    zh: '中文',
    en: 'English',
    notifications: '通知',
    clearAll: '清空',
    noNotifications: '暂无通知',
    appearance: '界面设置',
    appearanceDesc: '自定义您的使用体验',
    theme: '主题',
    themeDesc: '选择界面主题',
    themeLight: '浅色',
    themeDark: '深色',
    themeSystem: '跟随系统',
    selectLanguage: '选择界面语言',
    dataRefresh: '数据刷新',
    dataRefreshDesc: '控制数据自动刷新行为',
    refreshInterval: '刷新间隔',
    refreshIntervalDesc: '设置数据自动刷新频率',
    interval3s: '3 秒',
    interval5s: '5 秒',
    interval10s: '10 秒',
    interval30s: '30 秒',
    intervalOff: '关闭',
    notifyOnFailure: '失败通知',
    notifyOnFailureDesc: 'DAG 运行失败时提醒',
    systemInfo: '系统信息',
    systemInfoDesc: '关于 DAGForge 系统的详细信息',
    webUIVersion: 'Web UI 版本',
    apiEndpoint: 'API 端点',
    buildTime: '构建时间',
    themeUpdated: '主题已更新',
    languageSwitchedZh: '语言已切换为中文',
    languageSwitchedEn: 'Language switched to English',
    refreshDisabled: '已关闭自动刷新',
    refreshIntervalSet: '刷新间隔已设为 {seconds} 秒',
    notifyEnabled: '已开启失败通知',
    notifyDisabled: '已关闭失败通知',
  },
  notFound: {
    title: '页面未找到',
    message: '抱歉，未找到页面 {path}',
    backHome: '返回首页',
    viewDAGs: '查看 DAG 列表',
  },
  sidebar: {
    mainFeatures: '主要功能',
    system: '系统',
    apiConnected: 'API 已连接',
  },
  commandPalette: {
    placeholder: '搜索 DAG、任务、历史记录...',
    noResults: '未找到结果',
    navigation: '导航',
    recentRuns: '最近运行',
  },
};

const enTranslations: Translations = {
  common: {
    loading: 'Loading…',
    error: 'Error',
    success: 'Success',
    warning: 'Warning',
    confirm: 'Confirm',
    cancel: 'Cancel',
    save: 'Save',
    delete: 'Delete',
    edit: 'Edit',
    search: 'Search',
    refresh: 'Refresh',
    back: 'Back',
    viewAll: 'View all',
    noData: 'No data',
    run: 'Run',
    pause: 'Pause',
    resume: 'Resume',
    paused: 'Paused',
    home: 'Home',
    yes: 'Yes',
    no: 'No',
  },
  nav: {
    dashboard: 'Dashboard',
    dags: 'DAGs',
    history: 'History',
    settings: 'Settings',
  },
  dashboard: {
    title: 'Dashboard',
    subtitle: 'Scheduling overview',
    dagCount: 'DAGs',
    activeRuns: 'Active runs',
    totalTasks: 'Total Tasks',
    systemStatus: 'System Status',
    running: 'Running',
    stopped: 'Stopped',
    quickActions: 'Quick Actions',
    schedulerEngine: 'Scheduler Engine',
    dags: 'DAGs',
    failedTasks: 'Failed Tasks',
    successRate: 'Success Rate',
    moreMetrics: 'More Metrics',
    hideMetrics: 'Hide Details',
    searchPlaceholder: 'Search DAG name or description...',
    allDags: 'All',
    healthy: 'Healthy',
    abnormal: 'Abnormal',
    runHistory: 'Run History',
    runs: 'runs',
    noRecords: 'No records',
    queueBacklog: 'Queue Backlog',
    runningInstances: 'running',
    recentSuccessRate: 'Recent Success Rate',
    noMatchingDags: 'No matching DAGs found',
  },
  dags: {
    title: 'DAGs',
    subtitle: 'All workflows at a glance',
    searchPlaceholder: 'Search DAGs…',
    totalCount: '{count} DAGs',
    noDescription: 'No description',
    taskCount: '{count} tasks',
    trigger: 'Run',
    triggering: 'Triggering…',
    notFound: 'No matching DAGs found',
    adjustSearch: 'Try adjusting your search',
  },
  dagDetail: {
    backToDags: 'Back to DAG list',
    triggerRun: 'Trigger run',
    taskDefinitions: 'Task Definitions',
    flowGraph: 'Flow Graph',
    runInstances: 'Run instances',
    xcomData: 'XCom Data',
    xcomDescription: 'Data passed between tasks',
    selectRun: 'Select a run',
    noRuns: 'No run instances yet',
    clickTrigger: 'Click “Trigger run” to start',
    noXcom: 'No XCom data for this run',
    showingRunStatus: 'Showing status for Run #{number}',
    selectRunToView: 'Select a run to view status',
    started: 'Started',
    finished: 'Finished',
    dependsOn: 'Dependencies',
    executor: 'Executor',
    executionTime: 'Run time',
    variables: 'variables',
  },
  runStatus: {
    running: 'Running',
    success: 'Success',
    failed: 'Failed',
    pending: 'Pending',
    upstreamFailed: 'Upstream failed',
    retrying: 'Retrying',
    skipped: 'Skipped',
    noRun: 'Not run',
    inactive: 'Inactive',
  },
  triggerType: {
    manual: 'Manual',
    schedule: 'Schedule',
    api: 'API',
  },
  toast: {
    dagTriggered: 'DAG triggered',
    dagTriggerFailed: 'Failed to trigger DAG',
    newRunCreated: 'New run instance created',
    fetchDagsFailed: 'Failed to fetch DAG list',
    fetchDagDetailFailed: 'Failed to fetch DAG details',
    fetchHistoryFailed: 'Failed to fetch run history',
    unknownError: 'Unknown error',
    cannotTriggerEmptyDAG: 'Cannot trigger DAG',
    noDagTasks: 'This DAG has no tasks',
  },
  history: {
    title: 'Run history',
    subtitle: 'View DAG execution records',
    searchPlaceholder: 'Search DAGs…',
    allStatus: 'All statuses',
    noRecords: 'No records yet',
    executionDate: 'Execution time',
    tasks: 'tasks',
    failed: 'Failed',
    inProgress: 'In progress',
    duration: 'Duration',
    records: 'Records',
  },
  settings: {
    title: 'System settings',
    subtitle: 'Configuration & status',
    systemStatus: 'System Status',
    systemStatusDesc: 'Current system status',
    healthStatus: 'Health Status',
    healthy: 'Healthy',
    unhealthy: 'Unhealthy',
    lastUpdated: 'Last Updated',
    resourceStats: 'Resource Stats',
    resourceStatsDesc: 'DAG run stats',
    totalDAGs: 'Total DAGs',
    activeRuns: 'Active runs',
    language: 'Language',
    languageDesc: 'Select interface language',
    zh: '中文',
    en: 'English',
    notifications: 'Notifications',
    clearAll: 'Clear All',
    noNotifications: 'No notifications yet',
    appearance: 'Appearance',
    appearanceDesc: 'Customize your experience',
    theme: 'Theme',
    themeDesc: 'Select interface theme',
    themeLight: 'Light',
    themeDark: 'Dark',
    themeSystem: 'System',
    selectLanguage: 'Select interface language',
    dataRefresh: 'Data Refresh',
    dataRefreshDesc: 'Control automatic data refresh',
    refreshInterval: 'Refresh Interval',
    refreshIntervalDesc: 'Set data refresh frequency',
    interval3s: '3 seconds',
    interval5s: '5 seconds',
    interval10s: '10 seconds',
    interval30s: '30 seconds',
    intervalOff: 'Off',
    notifyOnFailure: 'Failure Notifications',
    notifyOnFailureDesc: 'Alert when DAG runs fail',
    systemInfo: 'System Information',
    systemInfoDesc: 'About DAGForge system',
    webUIVersion: 'Web UI Version',
    apiEndpoint: 'API Endpoint',
    buildTime: 'Build Time',
    themeUpdated: 'Theme updated',
    languageSwitchedZh: '语言已切换为中文',
    languageSwitchedEn: 'Language switched to English',
    refreshDisabled: 'Auto-refresh disabled',
    refreshIntervalSet: 'Refresh interval set to {seconds} seconds',
    notifyEnabled: 'Failure notifications enabled',
    notifyDisabled: 'Failure notifications disabled',
  },
  notFound: {
    title: 'Page not found',
    message: 'Sorry, we couldn’t find {path}',
    backHome: 'Back home',
    viewDAGs: 'View DAG list',
  },
  sidebar: {
    mainFeatures: 'Main features',
    system: 'System',
    apiConnected: 'API connected',
  },
  commandPalette: {
    placeholder: 'Search DAGs, tasks, history...',
    noResults: 'No results found',
    navigation: 'Navigation',
    recentRuns: 'Recent Runs',
  },
};

export const translations: Record<Locale, Translations> = {
  zh: zhTranslations,
  en: enTranslations,
};

export function interpolate(template: string, values: Record<string, string | number>): string {
  return template.replace(/\{(\w+)\}/g, (_, key) => String(values[key] ?? ''));
}
