import { type FC, useMemo } from 'react';
import {
  initData,
  type User,
  useSignal,
} from '@tma.js/sdk-react';
import { List, Placeholder } from '@telegram-apps/telegram-ui';

import { DisplayData, type DisplayDataRow } from '@/components/DisplayData/DisplayData.tsx';
import { Page } from '@/components/Page.tsx';

function toDisplayValue(value: unknown): DisplayDataRow['value'] {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (
    typeof value === 'string'
    || typeof value === 'number'
    || typeof value === 'boolean'
    || value === null
    || value === undefined
  ) {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return 'unserializable';
  }
}

function toEntries(value: unknown): Array<[string, unknown]> {
  if (!value || typeof value !== 'object') return [];
  return Object.entries(value as Record<string, unknown>);
}

function getUserRows(user: User): DisplayDataRow[] {
  return toEntries(user).map(([title, value]) => ({ title, value: toDisplayValue(value) }));
}

export const InitDataPage: FC = () => {
  const initDataRaw = useSignal(initData.raw);
  const initDataState = useSignal(initData.state) as Record<string, unknown> | undefined;

  const initDataRows = useMemo<DisplayDataRow[] | undefined>(() => {
    if (!initDataState || !initDataRaw) {
      return;
    }
    return [
      { title: 'raw', value: initDataRaw },
      ...toEntries(initDataState).reduce<DisplayDataRow[]>((acc, [title, value]) => {
        if (value instanceof Date) {
          acc.push({ title, value: value.toISOString() });
        } else if (!value || typeof value !== 'object') {
          acc.push({ title, value: toDisplayValue(value) });
        }
        return acc;
      }, []),
    ];
  }, [initDataState, initDataRaw]);

  const userRows = useMemo<DisplayDataRow[] | undefined>(() => {
    const user = initDataState?.user;
    return user && typeof user === 'object'
      ? getUserRows(user as User)
      : undefined;
  }, [initDataState]);

  const receiverRows = useMemo<DisplayDataRow[] | undefined>(() => {
    const receiver = initDataState?.receiver;
    return receiver && typeof receiver === 'object'
      ? getUserRows(receiver as User)
      : undefined;
  }, [initDataState]);

  const chatRows = useMemo<DisplayDataRow[] | undefined>(() => {
    const chat = initDataState?.chat;
    return !chat || typeof chat !== 'object'
      ? undefined
      : toEntries(chat).map(([title, value]) => ({ title, value: toDisplayValue(value) }));
  }, [initDataState]);

  if (!initDataRows) {
    return (
      <Page>
        <Placeholder
          header="Oops"
          description="Application was launched with missing init data"
        >
          <img
            alt="Telegram sticker"
            src="https://xelene.me/telegram.gif"
            style={{ display: 'block', width: '144px', height: '144px' }}
          />
        </Placeholder>
      </Page>
    );
  }
  return (
    <Page>
      <List>
        <DisplayData header={'Init Data'} rows={initDataRows}/>
        {userRows && <DisplayData header={'User'} rows={userRows}/>}
        {receiverRows && <DisplayData header={'Receiver'} rows={receiverRows}/>}
        {chatRows && <DisplayData header={'Chat'} rows={chatRows}/>}
      </List>
    </Page>
  );
};
