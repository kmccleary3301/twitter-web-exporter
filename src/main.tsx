import { render } from 'preact';
import { App } from './core/app';
import extensions from './core/extensions';

import BookmarksModule from './modules/bookmarks';
import CommunityMembersModule from './modules/community-members';
import CommunityTimelineModule from './modules/community-timeline';
import DirectMessagesModule from './modules/direct-messages';
import FollowersModule from './modules/followers';
import FollowingModule from './modules/following';
import HomeTimelineModule from './modules/home-timeline';
import LikesModule from './modules/likes';
import ListMembersModule from './modules/list-members';
import ListSubscribersModule from './modules/list-subscribers';
import ListTimelineModule from './modules/list-timeline';
import RetweetersModule from './modules/retweeters';
import RuntimeLogsModule from './modules/runtime-logs';
import SearchTimelineModule from './modules/search-timeline';
import TweetDetailModule from './modules/tweet-detail';
import UserDetailModule from './modules/user-detail';
import UserMediaModule from './modules/user-media';
import UserTweetsModule from './modules/user-tweets';

import './index.css';

const APP_ROOT_ID = 'twe-root';
const APP_ROOT_MOUNTED_FLAG = '__twe_root_mounted_v1';
const windowScope = globalThis as unknown as Record<string, unknown>;

extensions.add(FollowersModule);
extensions.add(FollowingModule);
extensions.add(UserDetailModule);
extensions.add(ListMembersModule);
extensions.add(ListSubscribersModule);
extensions.add(CommunityMembersModule);
extensions.add(RetweetersModule);
extensions.add(HomeTimelineModule);
extensions.add(ListTimelineModule);
extensions.add(CommunityTimelineModule);
extensions.add(BookmarksModule);
extensions.add(LikesModule);
extensions.add(UserTweetsModule);
extensions.add(UserMediaModule);
extensions.add(TweetDetailModule);
extensions.add(SearchTimelineModule);
extensions.add(DirectMessagesModule);
extensions.add(RuntimeLogsModule);
extensions.start();

function mountApp() {
  const existingRoot = document.getElementById(APP_ROOT_ID) as HTMLDivElement | null;
  if (windowScope[APP_ROOT_MOUNTED_FLAG]) {
    if (existingRoot) {
      return;
    }
    windowScope[APP_ROOT_MOUNTED_FLAG] = false;
  }

  const root = existingRoot ?? document.createElement('div');
  if (!existingRoot) {
    root.id = APP_ROOT_ID;
    document.body.append(root);
  }

  windowScope[APP_ROOT_MOUNTED_FLAG] = true;

  render(<App />, root);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', mountApp);
} else {
  mountApp();
}
