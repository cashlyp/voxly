import { hapticFeedback, popup } from '@tma.js/sdk-react';

type ConfirmOptions = {
  title: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
  destructive?: boolean;
};

export async function confirmAction(options: ConfirmOptions): Promise<boolean> {
  const {
    title,
    message,
    confirmText = 'Confirm',
    cancelText = 'Cancel',
    destructive = false,
  } = options;

  if (popup.show?.isAvailable?.()) {
    const result = await popup.show({
      title,
      message,
      buttons: [
        { id: 'cancel', type: 'default', text: cancelText },
        { id: 'confirm', type: destructive ? 'destructive' : 'default', text: confirmText },
      ],
    });
    return result === 'confirm';
  }

  return window.confirm(`${title}\n\n${message}`);
}

export function hapticImpact(style: 'light' | 'medium' | 'heavy' = 'medium') {
  if (hapticFeedback.impactOccurred?.isAvailable?.()) {
    hapticFeedback.impactOccurred(style);
  }
}

export function hapticSuccess() {
  if (hapticFeedback.notificationOccurred?.isAvailable?.()) {
    hapticFeedback.notificationOccurred('success');
  }
}

export function hapticError() {
  if (hapticFeedback.notificationOccurred?.isAvailable?.()) {
    hapticFeedback.notificationOccurred('error');
  }
}
