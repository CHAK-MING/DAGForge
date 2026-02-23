import { toast } from 'sonner';
import { useI18n } from '@/contexts/I18nContext';

export type NotificationType = 'success' | 'error' | 'warning' | 'info';

interface NotificationOptions {
  description?: string;
  duration?: number;
}

export function useNotification() {
  const { t } = useI18n();

  const notify = (type: NotificationType, message: string, options?: NotificationOptions) => {
    const toastOptions = {
      description: options?.description,
      duration: options?.duration,
    };

    switch (type) {
      case 'success':
        toast.success(message, toastOptions);
        break;
      case 'error':
        toast.error(message, toastOptions);
        break;
      case 'warning':
        toast.warning(message, toastOptions);
        break;
      case 'info':
        toast.info(message, toastOptions);
        break;
    }
  };

  return {
    success: (message: string, options?: NotificationOptions) => notify('success', message, options),
    error: (message: string, options?: NotificationOptions) => notify('error', message, options),
    warning: (message: string, options?: NotificationOptions) => notify('warning', message, options),
    info: (message: string, options?: NotificationOptions) => notify('info', message, options),
    
    // Predefined notifications
    dagTriggered: () => notify('success', t.toast.dagTriggered, { description: t.toast.newRunCreated }),
    dagTriggerFailed: (error?: string) => notify('error', t.toast.dagTriggerFailed, { 
      description: error || t.toast.unknownError 
    }),
    fetchDagsFailed: (error?: string) => notify('error', t.toast.fetchDagsFailed, { 
      description: error || t.toast.unknownError 
    }),
    fetchDagDetailFailed: (error?: string) => notify('error', t.toast.fetchDagDetailFailed, { 
      description: error || t.toast.unknownError 
    }),
    cannotTriggerEmptyDAG: () => notify('warning', t.toast.cannotTriggerEmptyDAG, { 
      description: t.toast.noDagTasks 
    }),
  };
}
