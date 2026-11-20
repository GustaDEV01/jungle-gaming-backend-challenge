import { type CanActivate, type ExecutionContext, Inject, Injectable } from '@nestjs/common';
import type { Request } from 'express';

export const PROVIDER_IDENTITY = Symbol('PROVIDER_IDENTITY');

export interface ProviderIdentity {
  /** Authenticated provider id (e.g. OIDC client `azp`), or null when anonymous. */
  providerId: string | null;
}

/**
 * Authentication extension point (README §2). Authentication is NOT
 * implemented in this challenge; see ARCHITECTURE.md "Autenticação".
 * A real implementation would validate an OIDC access token issued by an
 * external IdP (Keycloak/Zitadel, client-credentials per provider) and return
 * its client id.
 */
export interface ProviderIdentityPort {
  authenticate(request: Request): Promise<ProviderIdentity>;
}

/** No-op: every request is accepted as anonymous. */
export class NoopProviderIdentity implements ProviderIdentityPort {
  async authenticate(): Promise<ProviderIdentity> {
    return { providerId: null };
  }
}

/**
 * Guard applied to business endpoints (not to /health). With a real IdP it
 * would also enforce body.providerId === identity.providerId.
 */
@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(@Inject(PROVIDER_IDENTITY) private readonly identity: ProviderIdentityPort) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest<Request & { provider?: ProviderIdentity }>();
    req.provider = await this.identity.authenticate(req);
    return true;
  }
}
